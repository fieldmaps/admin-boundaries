# admin-boundaries

Pipeline that downloads, processes, and publishes global administrative boundary data.

## Pipeline Stages

```text
app/_01_download/    → fetch HDX GDB zip + metadata CSV to tmp/hdx/, extract GDB
app/_02_prepare/     → ingest GDB + COD/GeoBoundaries fallbacks into tmp/app.duckdb
app/_03_build/       → build adm{0..4}_{polygons,lines,points} tables in tmp/build.duckdb
app/_04_export/      → write GeoParquets, format conversions, area stats, and metadata indices
sync.py              → rclone push to Cloudflare R2 (standalone, NOT invoked by pipeline)
```

Entry point: `uv run python -m app` (all stages). Single stage: `uv run python -m app --step _01_download` (or `_02_prepare`, `_03_build`, `_04_export`); also honors `STEP` env var.

## Data Sources and Priority

| Priority    | Source                 | Input                                                             | Coverage                 |
| ----------- | ---------------------- | ----------------------------------------------------------------- | ------------------------ |
| 1 (highest) | HDX Global GDB         | extracted from `tmp/hdx/*.gdb.zip`                                | ~110 countries           |
| 2           | fieldmaps-COD fallback | `https://data.fieldmaps.io/cod/extended/{iso3}.parquet`           | COD countries not in GDB |
| 3           | GeoBoundaries          | `https://data.fieldmaps.io/geoboundaries/extended/{iso3}.parquet` | all remaining            |

ADM0 templates: `https://data.fieldmaps.io/adm0/osm/intl/adm0_{clip,polygons,lines,points}.parquet`

COD/GeoBoundaries fallbacks and ADM0 templates are streamed via httpfs — NOT pre-downloaded to disk.

## Key Constraints

- No PostgreSQL, no PostGIS, no Docker database service.
- Use `gdal vector convert` / `gdal vector info` (not `ogr2ogr` / `ogrinfo`).
- `_03_build` is global vectorized SQL — no per-country loops, no multiprocessing.
- DuckDB built-in GDAL (`FORMAT GDAL`) handles GPKG/GDB/XLSX export.
- Only the `admin4` GDB layer is read — it contains every COD country's leaf-level rows with NULLs for unused deeper levels.

## DuckDB Patterns

```python
conn = duckdb.connect()
conn.execute("LOAD spatial;")

# Read GeoParquet
conn.execute(f"CREATE TABLE src AS SELECT * FROM read_parquet('{path}')")

# Glob-merge (union_by_name handles column differences)
conn.execute(f"CREATE TABLE merged AS SELECT * FROM read_parquet('{dir}/*.parquet', union_by_name=true)")

# Write GeoParquet (PARQUET_OPTS defined in app/config.py)
PARQUET_OPTS = "(COMPRESSION ZSTD, COMPRESSION_LEVEL 15, GEOPARQUET_VERSION V2)"
conn.execute(f"COPY (SELECT * FROM t) TO '{dest}' {PARQUET_OPTS}")

# Export GPKG/GDB/XLSX via DuckDB's built-in GDAL
conn.execute(f"COPY (SELECT * FROM src) TO '{dest}' (FORMAT GDAL, DRIVER 'GPKG')")
```

## Development Commands

```bash
uv sync
uv run -m app
uv run -m app --step _01_download
uv run ruff check && uv run ruff format
```

## Editing Notes

A PostToolUse ruff hook runs automatically after every file write or edit. Because ruff removes unused imports immediately, **always include the usage of a new import in the same Edit/Write call** — never add imports in one edit and their usage in a follow-up edit, or ruff will strip them between the two calls.
