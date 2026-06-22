"""
Ingest pipeline orchestrator.

For each experiment (a pump.csv and/or scan.json whose filename encodes
metadata) this:
  1. parses the filename into structured metadata
  2. converts raw -> typed Parquet (pump + scan)
  3. computes cheap-to-filter SUMMARY STATS (velocity ranges, setSpeed range,
     waterValve states, channels present, duration, row counts)
  4. upserts one row into a DuckDB catalog table

The catalog is the small, indexable thing the API filters on. Bulk arrays
never enter the database -- they stay in Parquet, referenced by path.

Paths stored in the catalog are RELATIVE to the data dir (the catalog's parent
folder) so the corpus stays portable across machines / mapped drives. Keep the
Parquet tree under the same folder as catalog.duckdb.

Run:  python -m ingest.run_ingest  <raw_dir>  <parquet_dir>  <catalog.duckdb>
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pyarrow.compute as pc

from .convert import convert_pump_csv, convert_scan_json, write_parquet
from .filename_parser import parse_experiment_filename

CATALOG_DDL = """
CREATE TABLE IF NOT EXISTS experiments (
    experiment_id     VARCHAR PRIMARY KEY,   -- filename stem
    filename          VARCHAR,
    exp_date          DATE,
    board             VARCHAR,
    feed              BIGINT,
    set_speed         BIGINT,
    width             BIGINT,
    run               BIGINT,
    parse_ok          BOOLEAN,
    parse_warnings    VARCHAR,

    pump_parquet      VARCHAR,
    scan_parquet      VARCHAR,

    pump_rows         BIGINT,
    scan_rows         BIGINT,
    time_min          BIGINT,
    time_max          BIGINT,
    duration_s        DOUBLE,

    vx_min DOUBLE, vx_max DOUBLE, vx_mean DOUBLE,
    vy_min DOUBLE, vy_max DOUBLE, vy_mean DOUBLE,
    set_speed_min DOUBLE, set_speed_max DOUBLE,
    water_valve_min DOUBLE, water_valve_max DOUBLE,

    channels_present  VARCHAR,   -- comma list, lets API answer "no P0" w/o file open
    scan_channels     VARCHAR,
    pos_width         BIGINT,
    vel_width         BIGINT,
    ingested_at       TIMESTAMP DEFAULT now()
);
"""


def _safe_stats(table, col):
    if col not in table.column_names:
        return (None, None, None)
    arr = table[col]
    return (
        float(pc.min(arr).as_py()) if arr.length() else None,
        float(pc.max(arr).as_py()) if arr.length() else None,
        float(pc.mean(arr).as_py()) if arr.length() else None,
    )


def _rel(path: Path, data_dir: Path | None) -> str:
    """Store paths relative to the data dir so the catalog is portable.

    Falls back to an absolute path if the Parquet file lives outside the data
    dir (e.g. on a different drive), where a relative path can't be formed.
    """
    if data_dir is None:
        return str(path)
    try:
        return os.path.relpath(path, data_dir)
    except ValueError:
        return str(path)


def ingest_experiment(
    pump_csv: Path | None,
    scan_json: Path | None,
    parquet_dir: Path,
    con: duckdb.DuckDBPyConnection,
    data_dir: Path | None = None,
) -> dict:
    ref = pump_csv or scan_json
    meta = parse_experiment_filename(ref)
    exp_id = meta.stem
    out_dir = parquet_dir / exp_id
    rec: dict = meta.as_row()
    rec["experiment_id"] = exp_id

    pump_path = scan_path = None
    pump_rows = scan_rows = 0
    tmin = tmax = None
    vx = vy = (None, None, None)
    ss = (None, None, None)
    wv = (None, None, None)
    channels: list[str] = []
    scan_channels: list[str] = []
    pos_w = vel_w = None

    if pump_csv and Path(pump_csv).exists():
        ptbl, pman = convert_pump_csv(pump_csv)
        pump_abs = out_dir / "pump.parquet"
        write_parquet(ptbl, pump_abs)
        pump_path = _rel(pump_abs, data_dir)
        pump_rows = pman["rows"]
        tmin, tmax = pman["time_min"], pman["time_max"]
        channels = pman["columns"]
        pos_w = pman["array_widths"].get("pos")
        vel_w = pman["array_widths"].get("vel")
        # velocity components: vel0 = X velocity, vel1 = Y velocity
        vx = _safe_stats(ptbl, "vel0")
        vy = _safe_stats(ptbl, "vel1")
        ss = _safe_stats(ptbl, "setSpeed")
        wv = _safe_stats(ptbl, "waterValve")

    if scan_json and Path(scan_json).exists():
        stbl, sman = convert_scan_json(scan_json)
        scan_abs = out_dir / "scan.parquet"
        write_parquet(stbl, scan_abs)
        scan_path = _rel(scan_abs, data_dir)
        scan_rows = sman["rows"]
        scan_channels = sman["scan_channels"]
        if tmin is None:
            tmin, tmax = sman["time_min"], sman["time_max"]

    dur = (tmax - tmin) / 1e9 if (tmin is not None and tmax is not None) else None

    rec.update(
        dict(
            pump_parquet=pump_path, scan_parquet=scan_path,
            pump_rows=pump_rows, scan_rows=scan_rows,
            time_min=tmin, time_max=tmax, duration_s=dur,
            vx_min=vx[0], vx_max=vx[1], vx_mean=vx[2],
            vy_min=vy[0], vy_max=vy[1], vy_mean=vy[2],
            set_speed_min=ss[0], set_speed_max=ss[1],
            water_valve_min=wv[0], water_valve_max=wv[1],
            channels_present=",".join(channels),
            scan_channels=",".join(scan_channels),
            pos_width=pos_w, vel_width=vel_w,
        )
    )

    cols = [
        "experiment_id", "filename", "exp_date", "board", "feed",
        "set_speed", "width", "run", "parse_ok", "parse_warnings",
        "pump_parquet", "scan_parquet", "pump_rows", "scan_rows",
        "time_min", "time_max", "duration_s",
        "vx_min", "vx_max", "vx_mean", "vy_min", "vy_max", "vy_mean",
        "set_speed_min", "set_speed_max", "water_valve_min",
        "water_valve_max", "channels_present", "scan_channels",
        "pos_width", "vel_width",
    ]
    # Atomic upsert: a crash between DELETE and INSERT must not lose the row.
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "DELETE FROM experiments WHERE experiment_id = ?", [exp_id]
        )
        con.execute(
            f"INSERT INTO experiments ({','.join(cols)}) VALUES "
            f"({','.join(['?'] * len(cols))})",
            [rec.get(c) for c in cols],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return rec


def open_catalog(catalog_path: str | Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(catalog_path))
    con.execute(CATALOG_DDL)
    return con


def discover_pairs(raw_dir: Path):
    """Group raw files by stem, pairing each run's pump.csv with its scan.json.

    Supports two layouts:
      * two-folder:  <raw>/pump/<stem>.csv  +  <raw>/scan/<stem>.json
      * flat:        <raw>/<stem>.csv       +  <raw>/<stem>.json
    The two-folder layout wins if a 'pump' or 'scan' subfolder exists.
    Files are paired by identical stem; a run may have only one of the two.
    """
    by_stem: dict[str, dict] = {}
    pump_dir = raw_dir / "pump"
    scan_dir = raw_dir / "scan"

    if pump_dir.is_dir() or scan_dir.is_dir():
        if pump_dir.is_dir():
            for p in sorted(pump_dir.iterdir()):
                if p.suffix == ".csv":
                    by_stem.setdefault(p.stem, {})["pump"] = p
        if scan_dir.is_dir():
            for p in sorted(scan_dir.iterdir()):
                if p.suffix == ".json":
                    by_stem.setdefault(p.stem, {})["scan"] = p
    else:
        for p in sorted(raw_dir.iterdir()):
            if p.suffix == ".csv":
                by_stem.setdefault(p.stem, {})["pump"] = p
            elif p.suffix == ".json":
                by_stem.setdefault(p.stem, {})["scan"] = p
    return by_stem


if __name__ == "__main__":
    raw_dir = Path(sys.argv[1])
    parquet_dir = Path(sys.argv[2])
    catalog_path = sys.argv[3]
    # Data dir = the catalog's folder; stored Parquet paths are relative to it.
    data_dir = Path(catalog_path).resolve().parent
    con = open_catalog(catalog_path)
    pairs = discover_pairs(raw_dir)
    for stem, files in pairs.items():
        r = ingest_experiment(
            files.get("pump"), files.get("scan"), parquet_dir, con,
            data_dir=data_dir,
        )
        print(f"ingested {stem}: pump_rows={r['pump_rows']} "
              f"scan_rows={r['scan_rows']} dur={r['duration_s']}")
    con.close()
    print("catalog written:", catalog_path)
