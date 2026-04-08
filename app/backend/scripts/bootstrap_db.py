from __future__ import annotations

from pathlib import Path
import sys
import sqlite3

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.config import get_settings


BACKEND_DIR = Path(__file__).resolve().parents[1]


def main() -> None:
    settings = get_settings()
    sql_dir = BACKEND_DIR / "sql"
    schema_path = sql_dir / ("schema_sqlite.sql" if settings.database_backend == "sqlite" else "schema.sql")
    schema_sql = schema_path.read_text(encoding="utf-8")

    if settings.database_backend == "sqlite":
        sqlite_path = settings.database_url.replace("sqlite:///", "", 1)
        sqlite_file = Path(sqlite_path)
        if not sqlite_file.is_absolute():
            sqlite_file = (BACKEND_DIR / sqlite_file).resolve()
        sqlite_file.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(sqlite_file) as conn:
            conn.executescript(schema_sql)
            conn.commit()
        print(f"[ok] sqlite schema applied: {sqlite_file}")
        return

    import psycopg

    with psycopg.connect(settings.database_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()

    print(f"[ok] schema applied from {schema_path}")


if __name__ == "__main__":
    main()
