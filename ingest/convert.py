"""
Convert one experiment's raw pump.csv + scan.json into two typed Parquet files.

Design decisions (mapped to user requirements):
  1. Scan channels (x, z, m0, ...) stored as Parquet list<float32> columns
     -> always read back as whole arrays, never exploded to one-row-per-point.
  2. pos[] / vel[] exploded into real typed columns pos0..posN / vel0..velN.
  3. Per-file schema; produces a `channels_present` manifest for the catalog.
  4. Variable array width (pos was len-3, later len-5) and missing channels
     (no P0 in some files) handled per-file without a rigid global schema.

Requires pyarrow in the target environment:  pip install pyarrow
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# Columns in pump.csv that hold a stringified python list e.g. "[454.6,1160,...]"
ARRAY_COLUMNS = ("pos", "vel")
# Columns that are python bools spelled True/False
BOOL_COLUMNS = ("start", "waterPump", "solenoidValve", "mixer")
# Everything else numeric -> int or float, decided by sniffing.


def _parse_list_cell(cell: str) -> list[float]:
    """Parse a stringified list cell e.g. "[454.6,1160,...]" into floats.

    Tolerant of real-log values that ast.literal_eval rejects: nan/inf (float()
    accepts these), scientific notation, and stray whitespace.
    """
    cell = (cell or "").strip()
    if not cell:
        return []
    if cell[:1] == "[" and cell[-1:] == "]":
        cell = cell[1:-1]
    return [float(tok) for tok in cell.split(",") if tok.strip()]


def _coerce_scalar(v: str) -> Any:
    if v in ("True", "False"):
        return v == "True"
    try:
        iv = int(v)
        return iv
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v if v != "" else None


def convert_pump_csv(csv_path: str | Path) -> tuple[pa.Table, dict]:
    csv_path = Path(csv_path)
    with csv_path.open(newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    # Real logs get concatenated/restarted, re-emitting the header row (and the
    # odd blank line) mid-file. Drop those so they don't poison type coercion.
    ncols = len(header)
    rows = [
        r for r in rows
        if r and any(c.strip() for c in r) and r != header and len(r) == ncols
    ]

    n = len(rows)
    idx = {h: i for i, h in enumerate(header)}

    # Discover max width of each array column for THIS file (handles drift).
    arr_widths: dict[str, int] = {}
    parsed_arrays: dict[str, list[list[float]]] = {}
    for col in ARRAY_COLUMNS:
        if col not in idx:
            continue
        vals = [_parse_list_cell(r[idx[col]]) for r in rows]
        parsed_arrays[col] = vals
        arr_widths[col] = max((len(v) for v in vals), default=0)

    arrays: dict[str, pa.Array] = {}

    # timestamp -> int64 ns
    if "timestamp" in idx:
        ts = [int(r[idx["timestamp"]]) for r in rows]
        arrays["timestamp"] = pa.array(ts, type=pa.int64())

    # exploded array columns: pos0..posK
    for col, width in arr_widths.items():
        for k in range(width):
            colname = f"{col}{k}"
            colvals = [
                (v[k] if k < len(v) else None) for v in parsed_arrays[col]
            ]
            arrays[colname] = pa.array(colvals, type=pa.float64())

    # remaining scalar columns
    for h in header:
        if h == "timestamp" or h in ARRAY_COLUMNS:
            continue
        raw = [r[idx[h]] for r in rows]
        coerced = [_coerce_scalar(v) for v in raw]
        if h in BOOL_COLUMNS:
            arrays[h] = pa.array(
                [bool(x) if x is not None else None for x in coerced],
                type=pa.bool_(),
            )
        else:
            # int if every non-null value is int, else float
            non_null = [x for x in coerced if x is not None]
            all_int = non_null and all(isinstance(x, int) for x in non_null)
            arrays[h] = pa.array(
                coerced, type=pa.int64() if all_int else pa.float64()
            )

    table = pa.table(arrays)
    manifest = {
        "rows": n,
        "columns": list(arrays.keys()),
        "array_widths": arr_widths,
        "time_min": int(min(ts)) if "timestamp" in idx and n else None,
        "time_max": int(max(ts)) if "timestamp" in idx and n else None,
    }
    return table, manifest


def convert_scan_json(json_path: str | Path) -> tuple[pa.Table, dict]:
    json_path = Path(json_path)
    records: list[dict] = []
    with json_path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    n = len(records)
    if n == 0:
        return pa.table({}), {"rows": 0, "columns": [], "scan_channels": []}

    # Union of keys across all records (point 4: some files lack channels).
    all_keys: list[str] = []
    for rec in records:
        for k in rec:
            if k not in all_keys:
                all_keys.append(k)

    list_channels: list[str] = []
    scalar_keys: list[str] = []
    for k in all_keys:
        sample = next((r[k] for r in records if k in r), None)
        (list_channels if isinstance(sample, list) else scalar_keys).append(k)

    arrays: dict[str, pa.Array] = {}
    f32_list = pa.list_(pa.float32())

    for k in scalar_keys:
        vals = [rec.get(k) for rec in records]
        if k == "time":
            arrays[k] = pa.array([int(v) for v in vals], type=pa.int64())
        elif all(isinstance(v, bool) for v in vals if v is not None):
            arrays[k] = pa.array(vals, type=pa.bool_())
        elif all(
            isinstance(v, int) for v in vals if v is not None
        ):
            arrays[k] = pa.array(vals, type=pa.int64())
        else:
            arrays[k] = pa.array(vals, type=pa.float64())

    for k in list_channels:
        # float32 list column; missing -> null (channel absent in a record)
        col = [
            ([float(x) for x in rec[k]] if k in rec and rec[k] is not None
             else None)
            for rec in records
        ]
        arrays[k] = pa.array(col, type=f32_list)

    table = pa.table(arrays)
    times = arrays["time"].to_pylist() if "time" in arrays else []
    widths = {
        k: sorted({len(rec[k]) for rec in records if k in rec and rec[k]})
        for k in list_channels
    }
    manifest = {
        "rows": n,
        "columns": list(arrays.keys()),
        "scan_channels": list_channels,
        "scalar_keys": scalar_keys,
        "scan_array_widths": widths,
        "time_min": min(times) if times else None,
        "time_max": max(times) if times else None,
    }
    return table, manifest


def write_parquet(table: pa.Table, out_path: str | Path) -> int:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table, out_path, compression="zstd", compression_level=5,
        use_dictionary=True,
    )
    return out_path.stat().st_size
