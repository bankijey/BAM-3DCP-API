# 3DCP Sensor Data Platform

Turn messy, high-frequency industrial sensor logs into a **typed, columnar,
queryable** corpus — and serve it over a small HTTP API.

Built on real research data from a **3D concrete-printing (3DCP) rig** at BAM
(Germany's federal materials institute): ~50 Hz multi-sensor pump telemetry plus
a laser line-scanner, logged per print.

**Stack:** Python · PyArrow · Parquet (zstd) · DuckDB · FastAPI

---

## Why this exists

A single hour-long print emits 100M+ array floats across two asynchronous
sources in two awkward raw formats — a wide 50 Hz CSV and nested JSON-lines
laser profiles. Row databases bloat and scan slowly; loose files are
unsearchable. This is the **ingestion + storage + serving** layer that makes the
data usable:

- **Bronze → Parquet** — raw CSV/JSON parsed into typed, zstd-compressed,
  columnar files, one folder per print. Laser profiles stay as `list<float32>`
  arrays; `pos[]`/`vel[]` explode into real columns.
- **Catalog → DuckDB** — one tiny row per print (parsed metadata + summary
  stats + which channels exist). The index you query; bulk arrays never enter
  the database.
- **API → FastAPI** — filter the catalog, then pull only the columns / time
  window you need from Parquet (projection + predicate pushdown).

```
raw csv/json ──ingest──▶ parquet/<print>/{pump,scan}.parquet   (bulk, columnar)
                  └────▶ catalog.duckdb  (1 row/print: metadata + stats + paths)

FastAPI  /experiments               ── filter catalog ──▶ matching prints + paths
         /experiments/{id}/snapshot ── columnar read  ──▶ just the window asked for
```

## Quickstart

```bash
pip install -r requirements.txt

# 1. Ingest the included sample print (raw csv + json -> Parquet + catalog)
python -m ingest.run_ingest sample_data/raw sample_data/parquet sample_data/catalog.duckdb

# 2. Serve
CATALOG_PATH=sample_data/catalog.duckdb uvicorn api.app:app
# Windows PowerShell:  $env:CATALOG_PATH="sample_data/catalog.duckdb"; uvicorn api.app:app
```

Open http://localhost:8000/docs, or:

```
GET /experiments
GET /experiments/{id}
GET /experiments/{id}/snapshot?stream=pump&columns=timestamp,torque&t_start=..&t_end=..
GET /experiments/{id}/snapshot?stream=scan&columns=time,width
```

Catalog responses carry **human-readable run times** (derived from raw
nanosecond epochs, which are kept for precise time-window queries):

```json
{
  "experiment_id": "2025-10-23__BOARD__F10500__S0__W40__run_1",
  "pump_rows": 244017, "scan_rows": 147285,
  "start_time": "2025-10-23T13:17:29Z",
  "end_time":   "2025-10-23T14:38:49Z",
  "channels_present": "timestamp,pos0,...,torque,P0,P1,P2,..."
}
```

Missing data is handled gracefully — a pump-only print returns `data: []` for a
scan query, not an error.

## Engineering decisions (the interesting part)

- **Catalog / bulk split.** 300 prints = 300 tiny catalog rows + 300 Parquet
  folders. Search is one indexed query; you never scan hundreds of GB to find a
  run.
- **Parquet + zstd over a row store.** Columnar + dictionary/RLE gives ~3–11×
  compression *and* column/row pushdown — read just `torque`, or just a
  10-second window.
- **Schema-on-read, drift-tolerant.** Array widths auto-detected per file;
  missing channels recorded in `channels_present`, so the API answers "no P0
  here" without opening the file.
- **Real-world data quality.** Handles a saturation sentinel (`32768`), mid-file
  repeated headers (concatenated logs), `nan`/`inf` in numeric arrays, and
  asynchronous pump/scan clocks.
- **Portable paths.** The catalog stores Parquet paths relative to itself, so
  the corpus moves between machines / mounts without rewrites.

## What this demonstrates

Ingestion/ETL of messy multi-format sensor data · columnar lakehouse layout with
a metadata catalog · schema evolution & data-quality handling · query pushdown ·
a typed serving API · reproducible, one-command setup.

## Repo layout

```
ingest/filename_parser.py   tolerant filename → structured metadata
ingest/convert.py           csv + json → typed Parquet (PyArrow)
ingest/run_ingest.py        orchestrator + DuckDB catalog builder
api/app.py                  FastAPI: catalog search + columnar Parquet snapshot
sample_data/raw/            one real print (downsampled): pump csv + scan json
```

## Data & provenance

The sample is one print from a BAM 3DCP research rig (a `BOARD` specimen,
23 Oct 2025), **downsampled** for size. Sensor setup and method:
O. A. Jeyifous et al., *Investigating the impact of material rheology on
geometric accuracy in 3D concrete printing using real-time monitoring*,
NDT-CE 2025.

## Scope & roadmap

This is the **data-platform** layer. It is designed to extend upward into
analytics and process-state modelling (state estimation + change detection on
the same signals) surfaced through interactive dashboards — tracked separately.

---

*Author: Olubunmi Anthony Jeyifous — research associate, BAM (3D concrete
printing, real-time sensing & process control).*
