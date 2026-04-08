from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import geopandas as gpd
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
import requests
from shapely.geometry import mapping, shape


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MANIFEST_DIR = DATA_DIR / "manifests"

BOUNDARY_RAW_DIR = RAW_DIR / "boundaries"
DEM_RAW_DIR = RAW_DIR / "dem"
BAKER_RAW_DIR = RAW_DIR / "baker"

BOUNDARY_PROCESSED_DIR = PROCESSED_DIR / "boundaries"
DEM_PROCESSED_DIR = PROCESSED_DIR / "dem"
BAKER_PROCESSED_DIR = PROCESSED_DIR / "baker"

JIANGXI_NAME = "Jiangxi Province"
ADM1_URL = "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/gbOpen/CHN/ADM1/geoBoundaries-CHN-ADM1.geojson"
DEFAULT_PROXY = os.environ.get("GISDATAMONITOR_HTTP_PROXY", "").strip()

PC_SEARCH_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/search"
PC_TOKEN_URL = "https://planetarycomputer.microsoft.com/api/sas/v1/token/{token_path}"

ARCGIS_SEARCH_URL = "https://www.arcgis.com/sharing/rest/search"
ARCGIS_OWNER = "ces_ricegis"
REQUEST_HEADERS = {"User-Agent": "gisdatamonitor-jiangxi-seed/1.0"}

PREFERRED_SERVICE_PREFIXES = (
    "china_coal_power_plants",
    "china_gas_power_plants",
    "china_nuclear_power_plants",
    "china_solar_power_plants",
    "china_wind_power_plants",
    "chinaevb",
    "china_evb",
    "chinacrudepipelines",
    "chinarefinedproductpipelines",
    "chinanaturalgaspipelines",
    "chinaoilports",
    "chinaoilrefineries",
    "chinaoilstoragefacilities",
    "chinalngterminals",
    "globaldata_midstream_china_gas_storage",
)


DEM_CANDIDATES = (
    {
        "collection": "cop-dem-glo-30",
        "token_path": "elevationeuwest/copernicus-dem",
        "asset_key": "data",
        "priority": 1,
    },
    {
        "collection": "alos-dem",
        "token_path": "ai4edataeuwest/alos-dem",
        "asset_key": "data",
        "priority": 2,
    },
    {
        "collection": "nasadem",
        "token_path": "nasademeuwest/nasadem-cog",
        "asset_key": "elevation",
        "priority": 3,
    },
    {
        "collection": "cop-dem-glo-90",
        "token_path": "elevationeuwest/copernicus-dem",
        "asset_key": "data",
        "priority": 4,
    },
)


@dataclass
class ServiceRef:
    title: str
    item_id: str
    url: str

    @property
    def slug(self) -> str:
        parts = [segment for segment in self.url.rstrip("/").split("/") if segment]
        if len(parts) >= 2:
            return parts[-2]
        return self.title


def ensure_dirs() -> None:
    for path in [
        DATA_DIR,
        RAW_DIR,
        PROCESSED_DIR,
        MANIFEST_DIR,
        BOUNDARY_RAW_DIR,
        DEM_RAW_DIR,
        BAKER_RAW_DIR,
        BOUNDARY_PROCESSED_DIR,
        DEM_PROCESSED_DIR,
        BAKER_PROCESSED_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def build_session(proxy: str | None) -> requests.Session:
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or "layer"


def request_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_payload: dict[str, Any] | None = None,
    form_data: dict[str, Any] | None = None,
    method: str = "GET",
) -> dict[str, Any]:
    method = method.upper()
    if method == "POST":
        response = session.post(url, params=params, json=json_payload, data=form_data, timeout=120)
    else:
        response = session.get(url, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"Remote API error at {url}: {payload['error']}")
    return payload


def download_file(session: requests.Session, url: str, output_path: Path) -> Path:
    if output_path.exists() and output_path.stat().st_size > 0:
        print(f"[skip] {output_path.relative_to(ROOT)}")
        return output_path

    with session.get(url, stream=True, timeout=300) as response:
        response.raise_for_status()
        with output_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    print(f"[download] {output_path.relative_to(ROOT)}")
    return output_path


def save_json(output_path: Path, payload: Any) -> None:
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_jiangxi_boundary(session: requests.Session) -> gpd.GeoDataFrame:
    raw_path = BOUNDARY_RAW_DIR / "china_adm1.geojson"
    if not raw_path.exists():
        response = session.get(ADM1_URL, timeout=120)
        response.raise_for_status()
        raw_path.write_text(response.text, encoding="utf-8")
        print(f"[download] {raw_path.relative_to(ROOT)}")

    provinces = gpd.read_file(raw_path).to_crs(4326)
    jiangxi = provinces.loc[provinces["shapeName"].astype(str).eq(JIANGXI_NAME)].copy()
    if jiangxi.empty:
        raise RuntimeError("Failed to locate Jiangxi Province in geoBoundaries ADM1 source")

    jiangxi["name"] = "Jiangxi Province"
    jiangxi = jiangxi[["shapeGroup", "shapeName", "name", "geometry"]].rename(
        columns={"shapeGroup": "iso3", "shapeName": "source_name"}
    )
    jiangxi["geometry"] = jiangxi.geometry.make_valid()

    output_path = BOUNDARY_PROCESSED_DIR / "jiangxi_boundary.geojson"
    jiangxi.to_file(output_path, driver="GeoJSON")
    print(f"[write] {output_path.relative_to(ROOT)}")
    return jiangxi


def search_stac_items(session: requests.Session, collection: str, bbox: list[float]) -> list[dict[str, Any]]:
    payload = request_json(
        session,
        PC_SEARCH_URL,
        method="POST",
        json_payload={
            "collections": [collection],
            "bbox": bbox,
            "limit": 200,
        },
    )
    return payload.get("features") or []


def get_collection_metadata(session: requests.Session, collection: str) -> dict[str, Any]:
    return request_json(session, f"https://planetarycomputer.microsoft.com/api/stac/v1/collections/{collection}")


def resolve_dem_resolution_m(collection_meta: dict[str, Any], items: list[dict[str, Any]]) -> float:
    summaries = collection_meta.get("summaries")
    if isinstance(summaries, dict):
        gsd_values = summaries.get("gsd")
        if isinstance(gsd_values, list) and gsd_values:
            first = gsd_values[0]
            if isinstance(first, (int, float)):
                return float(first)

    if items:
        props = items[0].get("properties") or {}
        gsd = props.get("gsd")
        if isinstance(gsd, (int, float)):
            return float(gsd)

        transform = props.get("proj:transform")
        if isinstance(transform, list) and transform:
            pixel_size_deg = abs(float(transform[0]))
            return round(pixel_size_deg * 111_320, 2)

    raise RuntimeError(f"Failed to resolve DEM resolution for collection {collection_meta.get('id')}")


def choose_dem_source(session: requests.Session, bbox: list[float]) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    viable: list[tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]] = []
    for candidate in DEM_CANDIDATES:
        items = search_stac_items(session, candidate["collection"], bbox)
        if items:
            meta = get_collection_metadata(session, candidate["collection"])
            resolved = {
                **candidate,
                "title": str(meta.get("title") or candidate["collection"]),
                "resolution_m": resolve_dem_resolution_m(meta, items),
            }
            viable.append((resolved, items, meta))

    if not viable:
        raise RuntimeError("No DEM source returned coverage for Jiangxi Province")

    viable.sort(key=lambda row: (row[0]["resolution_m"], row[0]["priority"]))
    source, items, _ = viable[0]
    alternatives = [
        {
            "collection": row[0]["collection"],
            "title": row[0]["title"],
            "resolution_m": row[0]["resolution_m"],
            "priority": row[0]["priority"],
            "tile_count": len(row[1]),
        }
        for row in viable
    ]
    return source, items, alternatives


def get_pc_token(session: requests.Session, token_path: str) -> str:
    payload = request_json(session, PC_TOKEN_URL.format(token_path=token_path))
    token = str(payload.get("token") or "").strip()
    if not token:
        raise RuntimeError(f"Failed to retrieve Planetary Computer token for {token_path}")
    return token


def sign_asset_href(asset_href: str, token: str) -> str:
    separator = "&" if urlparse(asset_href).query else "?"
    return f"{asset_href}{separator}{token}"


def build_dem_dataset(
    session: requests.Session,
    jiangxi: gpd.GeoDataFrame,
) -> dict[str, Any]:
    bbox = [float(value) for value in jiangxi.total_bounds]
    source, items, alternatives = choose_dem_source(session, bbox)
    token = get_pc_token(session, source["token_path"])

    tile_dir = DEM_RAW_DIR / source["collection"] / "tiles"
    tile_dir.mkdir(parents=True, exist_ok=True)

    tile_paths: list[Path] = []
    for item in items:
        asset = item.get("assets", {}).get(source["asset_key"])
        if not isinstance(asset, dict) or not asset.get("href"):
            continue
        href = sign_asset_href(str(asset["href"]), token)
        output_path = tile_dir / f"{item['id']}.tif"
        download_file(session, href, output_path)
        tile_paths.append(output_path)

    if not tile_paths:
        raise RuntimeError("DEM search succeeded but no downloadable tiles were found")

    mosaic_dir = DEM_RAW_DIR / source["collection"] / "mosaic"
    mosaic_dir.mkdir(parents=True, exist_ok=True)
    mosaic_path = mosaic_dir / f"jiangxi_{source['collection']}_mosaic.tif"
    clipped_path = DEM_PROCESSED_DIR / f"jiangxi_{source['collection']}_{source['resolution_m']}m.tif"

    datasets = [rasterio.open(path) for path in tile_paths]
    try:
        mosaic_array, mosaic_transform = merge(datasets)
        meta = datasets[0].meta.copy()
        meta.update(
            {
                "driver": "GTiff",
                "height": mosaic_array.shape[1],
                "width": mosaic_array.shape[2],
                "transform": mosaic_transform,
                "compress": "LZW",
            }
        )
        with rasterio.open(mosaic_path, "w", **meta) as dst:
            dst.write(mosaic_array)

        geom = [mapping(jiangxi.geometry.union_all())]
        with rasterio.open(mosaic_path) as src:
            clipped_array, clipped_transform = mask(src, geom, crop=True)
            clipped_meta = src.meta.copy()
            clipped_meta.update(
                {
                    "driver": "GTiff",
                    "height": clipped_array.shape[1],
                    "width": clipped_array.shape[2],
                    "transform": clipped_transform,
                    "compress": "LZW",
                }
            )
            with rasterio.open(clipped_path, "w", **clipped_meta) as dst:
                dst.write(clipped_array)
    finally:
        for dataset in datasets:
            dataset.close()

    manifest = {
        "province": "Jiangxi",
        "source_collection": source["collection"],
        "source_title": source["title"],
        "resolution_m": source["resolution_m"],
        "selection_rule": "Choose the smallest available gsd/resolution among automatable official sources, then apply priority tie-breaker.",
        "alternatives": alternatives,
        "tile_count": len(tile_paths),
        "bbox": bbox,
        "raw_tiles": [str(path.relative_to(ROOT)).replace("\\", "/") for path in tile_paths],
        "mosaic_file": str(mosaic_path.relative_to(ROOT)).replace("\\", "/"),
        "clipped_file": str(clipped_path.relative_to(ROOT)).replace("\\", "/"),
    }
    save_json(MANIFEST_DIR / "jiangxi_dem_manifest.json", manifest)
    print(f"[write] {clipped_path.relative_to(ROOT)}")
    return manifest


def search_candidate_services(session: requests.Session) -> list[ServiceRef]:
    payload = request_json(
        session,
        ARCGIS_SEARCH_URL,
        params={
            "q": f'owner:{ARCGIS_OWNER} type:"Feature Service"',
            "f": "json",
            "num": 200,
            "sortField": "title",
            "sortOrder": "asc",
        },
    )

    services: list[ServiceRef] = []
    for row in payload.get("results") or []:
        title = str(row.get("title") or "")
        url = str(row.get("url") or "")
        item_id = str(row.get("id") or "")
        if not url or "FeatureServer" not in url:
            continue
        lower = title.lower()
        if "china" not in lower and "midstream_china_gas_storage" not in lower:
            continue
        services.append(ServiceRef(title=title, item_id=item_id, url=url))

    services.sort(
        key=lambda s: (
            0 if any(s.slug.lower().startswith(prefix) for prefix in PREFERRED_SERVICE_PREFIXES) else 1,
            s.title.lower(),
        )
    )
    return services


def detect_object_id_field(layer_meta: dict[str, Any]) -> str | None:
    object_id_field = layer_meta.get("objectIdField")
    if isinstance(object_id_field, str) and object_id_field:
        return object_id_field

    for field in layer_meta.get("fields") or []:
        if not isinstance(field, dict):
            continue
        if field.get("type") == "esriFieldTypeOID":
            name = field.get("name")
            if isinstance(name, str) and name:
                return name
    return None


def chunk_list(items: list[int], chunk_size: int) -> list[list[int]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def merge_geojson_features(target: dict[str, Any], payload: dict[str, Any]) -> None:
    features = payload.get("features") or []
    if isinstance(features, list):
        target["features"].extend(features)


def bbox_envelope(bounds: list[float]) -> str:
    xmin, ymin, xmax, ymax = bounds
    return json.dumps(
        {
            "xmin": xmin,
            "ymin": ymin,
            "xmax": xmax,
            "ymax": ymax,
            "spatialReference": {"wkid": 4326},
        },
        ensure_ascii=False,
    )


def query_layer_with_bbox(
    session: requests.Session,
    service_url: str,
    layer_id: int,
    bounds: list[float],
) -> dict[str, Any]:
    layer_base = f"{service_url.rstrip('/')}/{layer_id}"
    layer_meta = request_json(session, layer_base, params={"f": "json"})
    object_id_field = detect_object_id_field(layer_meta)
    max_record_count = int(layer_meta.get("maxRecordCount") or 1000)
    max_record_count = max(100, min(max_record_count, 500))
    geometry = bbox_envelope(bounds)

    common_params = {
        "geometry": geometry,
        "geometryType": "esriGeometryEnvelope",
        "inSR": 4326,
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "true",
        "outSR": 4326,
        "f": "geojson",
    }

    merged = {"type": "FeatureCollection", "features": []}

    if object_id_field:
        id_payload = request_json(
            session,
            f"{layer_base}/query",
            method="POST",
            form_data={
                "where": "1=1",
                "returnIdsOnly": "true",
                "geometry": geometry,
                "geometryType": "esriGeometryEnvelope",
                "inSR": 4326,
                "spatialRel": "esriSpatialRelIntersects",
                "f": "json",
            },
        )
        object_ids = sorted(id_payload.get("objectIds") or [])
        for chunk in chunk_list(object_ids, max_record_count):
            payload = request_json(
                session,
                f"{layer_base}/query",
                method="POST",
                form_data={
                    **common_params,
                    "where": f"{object_id_field} IN ({','.join(str(oid) for oid in chunk)})",
                    "outFields": "*",
                },
            )
            merge_geojson_features(merged, payload)
        return merged

    offset = 0
    while True:
        payload = request_json(
            session,
            f"{layer_base}/query",
            method="POST",
            form_data={
                **common_params,
                "where": "1=1",
                "outFields": "*",
                "resultOffset": offset,
                "resultRecordCount": max_record_count,
            },
        )
        features = payload.get("features") or []
        if not features:
            break
        merge_geojson_features(merged, payload)
        exceeded = bool((payload.get("properties") or {}).get("exceededTransferLimit"))
        if not exceeded or len(features) < max_record_count:
            break
        offset += len(features)

    return merged


def feature_collection_to_gdf(payload: dict[str, Any]) -> gpd.GeoDataFrame:
    features = payload.get("features") or []
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs=4326)
    gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
    if gdf.empty:
        return gdf
    gdf["geometry"] = gdf.geometry.make_valid()
    return gdf


def filter_baker_to_jiangxi(
    session: requests.Session,
    jiangxi: gpd.GeoDataFrame,
) -> dict[str, Any]:
    services = search_candidate_services(session)
    bounds = [float(value) for value in jiangxi.total_bounds]
    province_geom = jiangxi.geometry.union_all()

    layer_manifest: list[dict[str, Any]] = []
    bbox_raw_dir = BAKER_RAW_DIR / "jiangxi_bbox"
    bbox_raw_dir.mkdir(parents=True, exist_ok=True)

    for service in services:
        service_meta = request_json(session, service.url.rstrip("/"), params={"f": "json"})
        for layer in service_meta.get("layers") or []:
            layer_id = int(layer.get("id"))
            layer_name = str(layer.get("name") or f"layer_{layer_id}")
            slug = sanitize_filename(f"{service.slug}__{layer_name}")
            raw_path = bbox_raw_dir / f"{slug}.geojson"
            processed_path = BAKER_PROCESSED_DIR / f"{slug}.geojson"

            payload = query_layer_with_bbox(session, service.url, layer_id, bounds)
            raw_count = len(payload.get("features") or [])
            if raw_count == 0:
                continue

            raw_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            print(f"[write] {raw_path.relative_to(ROOT)}")

            gdf = feature_collection_to_gdf(payload)
            if gdf.empty:
                continue

            clipped = gdf.clip(province_geom)
            clipped = clipped.loc[~clipped.geometry.is_empty].copy()
            if clipped.empty:
                continue

            clipped.to_file(processed_path, driver="GeoJSON")
            print(f"[write] {processed_path.relative_to(ROOT)}")

            geometry_types = sorted({geom.geom_type for geom in clipped.geometry if geom is not None})
            layer_manifest.append(
                {
                    "service_title": service.title,
                    "service_url": service.url,
                    "service_item_id": service.item_id,
                    "service_slug": service.slug,
                    "layer_id": layer_id,
                    "layer_name": layer_name,
                    "raw_bbox_feature_count": raw_count,
                    "jiangxi_feature_count": int(len(clipped)),
                    "geometry_types": geometry_types,
                    "raw_file": str(raw_path.relative_to(ROOT)).replace("\\", "/"),
                    "processed_file": str(processed_path.relative_to(ROOT)).replace("\\", "/"),
                }
            )

    manifest = {
        "province": "Jiangxi",
        "source_owner": ARCGIS_OWNER,
        "service_count": len(services),
        "downloaded_layer_count": len(layer_manifest),
        "layers": layer_manifest,
    }
    save_json(MANIFEST_DIR / "jiangxi_baker_manifest.json", manifest)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Jiangxi-only DEM and Baker seed data")
    parser.add_argument(
        "--proxy",
        default=DEFAULT_PROXY,
        help="HTTP/HTTPS proxy, e.g. http://127.0.0.1:7897",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()
    session = build_session(args.proxy)
    if args.proxy:
        print(f"[info] using proxy: {args.proxy}")

    jiangxi = load_jiangxi_boundary(session)
    dem_manifest = build_dem_dataset(session, jiangxi)
    baker_manifest = filter_baker_to_jiangxi(session, jiangxi)

    summary = {
        "province": "Jiangxi",
        "proxy": args.proxy or None,
        "dem": dem_manifest,
        "baker": {
            "downloaded_layer_count": baker_manifest["downloaded_layer_count"],
            "manifest_file": "data/manifests/jiangxi_baker_manifest.json",
        },
    }
    save_json(MANIFEST_DIR / "jiangxi_seed_summary.json", summary)
    print("[done] Jiangxi seed data prepared")


if __name__ == "__main__":
    main()
