from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import geopandas as gpd
import numpy as np
import rasterio
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.db import get_engine  # noqa: E402


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"
MANIFEST_DIR = DATA_DIR / "manifests"
PROCESSED_DIR = DATA_DIR / "processed"

BOUNDARY_FILE = PROCESSED_DIR / "boundaries" / "jiangxi_boundary.geojson"
DEM_MANIFEST_FILE = MANIFEST_DIR / "jiangxi_dem_manifest.json"
BAKER_MANIFEST_FILE = MANIFEST_DIR / "jiangxi_baker_manifest.json"
DERIVATIVE_DIR = PROCESSED_DIR / "dem" / "derivatives"
YEAR_PATTERN = re.compile(r"(19\d{2}|20\d{2})")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_properties(properties: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in properties.items():
        out[str(key)] = value if isinstance(value, (str, int, float, bool)) or value is None else str(value)
    return out


def pick_case_insensitive(properties: dict[str, Any], candidates: tuple[str, ...]) -> Any:
    lowered = {str(key).lower(): key for key in properties}
    for candidate in candidates:
        original = lowered.get(candidate.lower())
        if original is not None:
            return properties.get(original)
    return None


def parse_year(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and 1900 <= value <= 2100:
        return value
    matches = YEAR_PATTERN.findall(str(value))
    for match in matches:
        year = int(match)
        if 1900 <= year <= 2100:
            return year
    return None


def extract_start_year(properties: dict[str, Any]) -> int | None:
    for key, value in properties.items():
        key_lower = str(key).lower()
        if any(token in key_lower for token in ("start", "year", "commission", "operate", "online")):
            year = parse_year(value)
            if year is not None:
                return year
    for value in properties.values():
        year = parse_year(value)
        if year is not None:
            return year
    return None


def extract_status(properties: dict[str, Any]) -> str | None:
    for key, value in properties.items():
        if any(token in str(key).lower() for token in ("status", "state", "phase", "operat")):
            text_value = "" if value is None else str(value).strip()
            if text_value:
                return text_value
    return None


def extract_city(properties: dict[str, Any]) -> str | None:
    value = pick_case_insensitive(
        properties,
        ("city", "prefecture", "prefecture_city", "city_name", "adm2", "行政区", "地市", "市"),
    )
    if value is None:
        return None
    city = str(value).strip()
    return city or None


def extract_name(properties: dict[str, Any]) -> str | None:
    value = pick_case_insensitive(properties, ("name", "facility_name", "plant_name", "project_name", "title"))
    if value is None:
        return None
    name = str(value).strip()
    return name or None


def facility_type_from_source(source_layer: str) -> str:
    lower = source_layer.lower()
    if "coal_power" in lower:
        return "coal_power_plant"
    if "gas_power" in lower:
        return "gas_power_plant"
    if "nuclear" in lower:
        return "nuclear_power_plant"
    if "solar" in lower:
        return "solar_power_plant"
    if "wind" in lower:
        return "wind_power_plant"
    if "evb" in lower:
        return "battery_factory"
    if "crudepipeline" in lower:
        return "crude_pipeline"
    if "naturalgaspipeline" in lower:
        return "gas_pipeline"
    if "refinedproductpipeline" in lower:
        return "refined_pipeline"
    if "oilrefiner" in lower:
        return "oil_refinery"
    if "oilstorage" in lower:
        return "oil_storage"
    return "energy_facility"


def upsert_data_version(dataset_key: str, dataset_version: str, metadata: dict[str, Any]) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO data_versions (dataset_key, dataset_version, metadata, updated_at)
                VALUES (:dataset_key, :dataset_version, :metadata, CURRENT_TIMESTAMP)
                ON CONFLICT(dataset_key)
                DO UPDATE SET
                    dataset_version = excluded.dataset_version,
                    metadata = excluded.metadata,
                    updated_at = CURRENT_TIMESTAMP
                """
            ),
            {
                "dataset_key": dataset_key,
                "dataset_version": dataset_version,
                "metadata": json.dumps(metadata, ensure_ascii=False),
            },
        )


def clear_tables(*tables: str) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        for table in tables:
            conn.execute(text(f"DELETE FROM {table}"))


def ingest_boundary() -> None:
    gdf = gpd.read_file(BOUNDARY_FILE).to_crs(4326)
    clear_tables("boundary_jx")
    engine = get_engine()
    with engine.begin() as conn:
        for row in gdf.itertuples():
            geom = row.geometry
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])
            conn.execute(
                text(
                    """
                    INSERT INTO boundary_jx (name, iso3, source_name, geom_json, created_at)
                    VALUES (:name, :iso3, :source_name, :geom_json, CURRENT_TIMESTAMP)
                    """
                ),
                {
                    "name": str(getattr(row, "name", "Jiangxi Province")),
                    "iso3": str(getattr(row, "iso3", "CHN")),
                    "source_name": str(getattr(row, "source_name", "Jiangxi Province")),
                    "geom_json": json.dumps(mapping(geom), ensure_ascii=False),
                },
            )
    upsert_data_version(
        dataset_key="boundary_jx",
        dataset_version="v1",
        metadata={"feature_count": len(gdf), "source": str(BOUNDARY_FILE.relative_to(PROJECT_ROOT))},
    )
    print(f"[ok] boundary imported: {len(gdf)}")


def _write_float32_geotiff(output_path: Path, layer: np.ndarray, profile: dict[str, Any], nodata_value: float = -9999.0) -> None:
    out = np.nan_to_num(layer, nan=nodata_value, posinf=nodata_value, neginf=nodata_value).astype("float32")
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(out, 1)


def compute_dem_derivatives(dem_file: Path) -> dict[str, Path]:
    DERIVATIVE_DIR.mkdir(parents=True, exist_ok=True)
    output_paths = {
        "slope": DERIVATIVE_DIR / "jiangxi_slope.tif",
        "aspect": DERIVATIVE_DIR / "jiangxi_aspect.tif",
        "hillshade": DERIVATIVE_DIR / "jiangxi_hillshade.tif",
        "roughness": DERIVATIVE_DIR / "jiangxi_roughness.tif",
    }

    print("[step] dem derivatives: reading DEM raster", flush=True)
    with rasterio.open(dem_file) as src:
        dem = src.read(1).astype("float32")
        if src.nodata is not None:
            dem = np.where(dem == src.nodata, np.nan, dem)

        x_res = abs(float(src.transform.a))
        y_res = abs(float(src.transform.e))
        profile = src.profile.copy()
        profile.update(dtype="float32", count=1, compress="LZW", nodata=-9999.0)

        print("[step] dem derivatives: computing gradients", flush=True)
        grad_y, grad_x = np.gradient(dem, y_res, x_res)

        print("[step] dem derivatives: writing slope", flush=True)
        slope = np.degrees(np.arctan(np.sqrt(np.square(grad_x) + np.square(grad_y))))
        _write_float32_geotiff(output_paths["slope"], slope, profile)
        del slope

        print("[step] dem derivatives: writing aspect", flush=True)
        aspect = np.degrees(np.arctan2(-grad_x, grad_y))
        aspect = np.where(aspect < 0, 360.0 + aspect, aspect)
        _write_float32_geotiff(output_paths["aspect"], aspect, profile)
        del aspect

        print("[step] dem derivatives: writing hillshade", flush=True)
        azimuth = np.radians(315.0)
        zenith = np.radians(45.0)
        slope_rad = np.arctan(np.sqrt(np.square(grad_x) + np.square(grad_y)))
        aspect_rad = np.arctan2(-grad_x, grad_y)
        hillshade = 255.0 * (
            np.cos(zenith) * np.cos(slope_rad)
            + np.sin(zenith) * np.sin(slope_rad) * np.cos(azimuth - aspect_rad)
        )
        hillshade = np.clip(hillshade, 0, 255)
        _write_float32_geotiff(output_paths["hillshade"], hillshade, profile)
        del hillshade

        # Roughness approximation for very large rasters to avoid extreme memory/time usage.
        print("[step] dem derivatives: writing roughness", flush=True)
        roughness = np.sqrt(np.square(grad_x) + np.square(grad_y))
        _write_float32_geotiff(output_paths["roughness"], roughness, profile)

        del roughness
        del grad_x
        del grad_y
        del dem

    print("[step] dem derivatives: done", flush=True)
    return output_paths


def ingest_dem() -> tuple[Path, dict[str, Path]]:
    manifest = load_json(DEM_MANIFEST_FILE)
    clipped_file = PROJECT_ROOT / manifest["clipped_file"]
    print(f"[step] dem ingest: using clipped DEM {clipped_file}", flush=True)
    derivatives = compute_dem_derivatives(clipped_file)
    print("[step] dem ingest: writing DEM metadata tables", flush=True)
    clear_tables("dem_tiles", "dem_derivatives")
    engine = get_engine()
    with engine.begin() as conn:
        for tile in manifest["raw_tiles"]:
            tile_path = PROJECT_ROOT / tile
            if not tile_path.exists():
                continue
            with rasterio.open(tile_path) as src:
                bounds = src.bounds
                bbox_poly = Polygon(
                    [
                        (bounds.left, bounds.bottom),
                        (bounds.left, bounds.top),
                        (bounds.right, bounds.top),
                        (bounds.right, bounds.bottom),
                        (bounds.left, bounds.bottom),
                    ]
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO dem_tiles (
                            source_collection, source_title, tile_name, tile_path, resolution_m, width, height, bbox_json, loaded_at
                        ) VALUES (
                            :source_collection, :source_title, :tile_name, :tile_path, :resolution_m, :width, :height, :bbox_json, CURRENT_TIMESTAMP
                        )
                        """
                    ),
                    {
                        "source_collection": manifest["source_collection"],
                        "source_title": manifest["source_title"],
                        "tile_name": tile_path.name,
                        "tile_path": str(tile_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                        "resolution_m": float(manifest["resolution_m"]),
                        "width": int(src.width),
                        "height": int(src.height),
                        "bbox_json": json.dumps(mapping(bbox_poly), ensure_ascii=False),
                    },
                )
        with rasterio.open(clipped_file) as src:
            bounds = src.bounds
            bbox_poly = Polygon(
                [
                    (bounds.left, bounds.bottom),
                    (bounds.left, bounds.top),
                    (bounds.right, bounds.top),
                    (bounds.right, bounds.bottom),
                    (bounds.left, bounds.bottom),
                ]
            )
            for derivative_name, derivative_path in derivatives.items():
                conn.execute(
                    text(
                        """
                        INSERT INTO dem_derivatives (
                            derivative_type, raster_path, resolution_m, width, height, bbox_json, metadata, loaded_at
                        ) VALUES (
                            :derivative_type, :raster_path, :resolution_m, :width, :height, :bbox_json, :metadata, CURRENT_TIMESTAMP
                        )
                        """
                    ),
                    {
                        "derivative_type": derivative_name,
                        "raster_path": str(derivative_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                        "resolution_m": float(manifest["resolution_m"]),
                        "width": int(src.width),
                        "height": int(src.height),
                        "bbox_json": json.dumps(mapping(bbox_poly), ensure_ascii=False),
                        "metadata": json.dumps(
                            {
                                "source_collection": manifest["source_collection"],
                                "source_title": manifest["source_title"],
                            },
                            ensure_ascii=False,
                        ),
                    },
                )
    upsert_data_version(
        dataset_key="dem_jiangxi",
        dataset_version=f"{manifest['source_collection']}_{manifest['resolution_m']}m",
        metadata={
            "tile_count": int(manifest["tile_count"]),
            "resolution_m": float(manifest["resolution_m"]),
            "source_collection": manifest["source_collection"],
            "source_title": manifest["source_title"],
            "clipped_file": manifest["clipped_file"],
            "derivatives": [str(path.relative_to(PROJECT_ROOT)).replace("\\", "/") for path in derivatives.values()],
        },
    )
    print(f"[ok] dem imported: {manifest['tile_count']} tiles + 4 derivatives")
    return clipped_file, derivatives


def ingest_baker() -> None:
    manifest = load_json(BAKER_MANIFEST_FILE)
    clear_tables("facility_terrain_metrics", "baker_facilities")
    feature_count = 0
    engine = get_engine()
    with engine.begin() as conn:
        for layer in manifest["layers"]:
            processed_file = PROJECT_ROOT / layer["processed_file"]
            if not processed_file.exists():
                continue
            source_layer = processed_file.stem
            gdf = gpd.read_file(processed_file).to_crs(4326)
            for idx, row in gdf.iterrows():
                geometry = row.geometry
                if geometry is None or geometry.is_empty:
                    continue
                geometry = geometry.buffer(0) if not geometry.is_valid else geometry
                if geometry.is_empty:
                    continue
                properties = normalize_properties(row.drop(labels=["geometry"]).to_dict())
                source_id = pick_case_insensitive(properties, ("objectid", "id", "fid", "globalid", "gid")) or str(idx + 1)
                conn.execute(
                    text(
                        """
                        INSERT INTO baker_facilities (
                            id, facility_id, facility_type, source_layer, name, start_year, status, admin_city, properties, geom_json, created_at
                        ) VALUES (
                            :id, :facility_id, :facility_type, :source_layer, :name, :start_year, :status, :admin_city, :properties, :geom_json, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT(facility_id)
                        DO UPDATE SET
                            facility_type = excluded.facility_type,
                            source_layer = excluded.source_layer,
                            name = excluded.name,
                            start_year = excluded.start_year,
                            status = excluded.status,
                            admin_city = excluded.admin_city,
                            properties = excluded.properties,
                            geom_json = excluded.geom_json
                        """
                    ),
                    {
                        "id": str(uuid4()),
                        "facility_id": f"{source_layer}:{source_id}",
                        "facility_type": facility_type_from_source(source_layer),
                        "source_layer": source_layer,
                        "name": extract_name(properties),
                        "start_year": extract_start_year(properties),
                        "status": extract_status(properties),
                        "admin_city": extract_city(properties),
                        "properties": json.dumps(properties, ensure_ascii=False),
                        "geom_json": json.dumps(mapping(geometry), ensure_ascii=False),
                    },
                )
                feature_count += 1
    upsert_data_version(
        dataset_key="baker_jiangxi",
        dataset_version="v2026_mvp",
        metadata={
            "layer_count": int(manifest["downloaded_layer_count"]),
            "feature_count": feature_count,
            "manifest": "data/manifests/jiangxi_baker_manifest.json",
        },
    )
    print(f"[ok] baker imported: {feature_count} features")


def sample_raster(src: rasterio.io.DatasetReader, lon: float, lat: float) -> float | None:
    value = list(src.sample([(lon, lat)]))[0][0]
    number = float(value)
    if src.nodata is not None and np.isclose(number, float(src.nodata)):
        return None
    return None if np.isnan(number) else number


def build_facility_terrain_metrics(dem_file: Path, derivatives: dict[str, Path]) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        facilities = conn.execute(text("SELECT id, geom_json FROM baker_facilities")).mappings().all()
    rows: list[dict[str, Any]] = []
    with rasterio.open(dem_file) as dem_src, rasterio.open(derivatives["slope"]) as slope_src, rasterio.open(
        derivatives["aspect"]
    ) as aspect_src, rasterio.open(derivatives["hillshade"]) as hillshade_src, rasterio.open(
        derivatives["roughness"]
    ) as roughness_src:
        for facility in facilities:
            point = shape(json.loads(facility["geom_json"])).representative_point()
            lon, lat = float(point.x), float(point.y)
            rows.append(
                {
                    "facility_pk": facility["id"],
                    "elevation_m": sample_raster(dem_src, lon, lat),
                    "slope_deg": sample_raster(slope_src, lon, lat),
                    "aspect_deg": sample_raster(aspect_src, lon, lat),
                    "hillshade": sample_raster(hillshade_src, lon, lat),
                    "roughness": sample_raster(roughness_src, lon, lat),
                }
            )
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO facility_terrain_metrics (
                        facility_pk, elevation_m, slope_deg, aspect_deg, hillshade, roughness, computed_at
                    ) VALUES (
                        :facility_pk, :elevation_m, :slope_deg, :aspect_deg, :hillshade, :roughness, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(facility_pk)
                    DO UPDATE SET
                        elevation_m = excluded.elevation_m,
                        slope_deg = excluded.slope_deg,
                        aspect_deg = excluded.aspect_deg,
                        hillshade = excluded.hillshade,
                        roughness = excluded.roughness,
                        computed_at = CURRENT_TIMESTAMP
                    """
                ),
                row,
            )
    print(f"[ok] facility terrain metrics computed: {len(rows)}")


def write_integrity_version() -> None:
    dem_manifest = load_json(DEM_MANIFEST_FILE)
    baker_manifest = load_json(BAKER_MANIFEST_FILE)
    upsert_data_version(
        dataset_key="seed_integrity",
        dataset_version="jiangxi_mvp",
        metadata={
            "dem_source": dem_manifest["source_collection"],
            "dem_resolution_m": dem_manifest["resolution_m"],
            "dem_tile_count": dem_manifest["tile_count"],
            "baker_layer_count": baker_manifest["downloaded_layer_count"],
            "baker_feature_count_manifest": sum(layer["jiangxi_feature_count"] for layer in baker_manifest["layers"]),
        },
    )


def main() -> None:
    ingest_boundary()
    dem_file, derivatives = ingest_dem()
    ingest_baker()
    build_facility_terrain_metrics(dem_file, derivatives)
    write_integrity_version()
    print("[done] static datasets loaded into SQLite")


if __name__ == "__main__":
    main()
