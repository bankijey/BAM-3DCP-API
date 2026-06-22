"""
FastAPI service for 3DCP experiment retrieval.

Two-stage design:
  /experiments  -> filters the small DuckDB CATALOG (fast, indexed).
                   Query params: date range, board, feed/setSpeed,
                   X/Y velocity with gt|lt|eq comparison, waterValve,
                   required channels.
  /experiments/{id}/snapshot -> opens only the matched Parquet file(s)
                   and reads only the requested columns / time window.

Bulk arrays never travel through the database; the catalog returns paths,
the snapshot endpoint does a columnar Parquet read.

Run:  uvicorn api.app:app --reload
Env:  CATALOG_PATH=/path/to/catalog.duckdb
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query

CATALOG_PATH = os.environ.get("CATALOG_PATH", "data/catalog.duckdb")

app = FastAPI(title="3DCP Experiment API", version="1.0")


def _con() -> duckdb.DuckDBPyConnection:
    # read_only so many API workers can share the catalog safely
    return duckdb.connect(CATALOG_PATH, read_only=True)


class Cmp(str, Enum):
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    eq = "eq"


_OP = {Cmp.gt: ">", Cmp.gte: ">=", Cmp.lt: "<", Cmp.lte: "<=", Cmp.eq: "="}


def _add_human_times(row: dict[str, Any]) -> dict[str, Any]:
    """Add human-readable start_time / end_time (UTC ISO-8601) from the catalog's
    time_min / time_max nanosecond epochs. Raw ns fields are kept so callers can
    still build /snapshot time-window queries."""
    for src, dst in (("time_min", "start_time"), ("time_max", "end_time")):
        ns = row.get(src)
        row[dst] = (
            datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
            .isoformat(timespec="seconds").replace("+00:00", "Z")
            if ns is not None else None
        )
    return row


@app.get("/experiments")
def search_experiments(
    date_from: str | None = Query(None, description="YYYY-MM-DD inclusive"),
    date_to: str | None = Query(None, description="YYYY-MM-DD inclusive"),
    board: str | None = None,
    feed: int | None = None,
    set_speed: int | None = None,
    set_speed_cmp: Cmp = Cmp.eq,
    # X / Y velocity resolution filter with comparison operator.
    # Compares against the run's velocity RANGE in the catalog:
    #   gt  -> vx_max >  value   (run reaches above this speed)
    #   lt  -> vx_min <  value   (run goes below this speed)
    #   eq  -> value within [vx_min, vx_max]
    vx: float | None = None,
    vx_cmp: Cmp = Cmp.gte,
    vy: float | None = None,
    vy_cmp: Cmp = Cmp.gte,
    water_valve: float | None = None,
    water_valve_cmp: Cmp = Cmp.eq,
    require_channels: str | None = Query(
        None, description="comma list, e.g. P0,T1 -> only runs having them"
    ),
    limit: int = 200,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []

    if date_from:
        where.append("exp_date >= ?"); params.append(date_from)
    if date_to:
        where.append("exp_date <= ?"); params.append(date_to)
    if board:
        where.append("board = ?"); params.append(board)
    if feed is not None:
        where.append("feed = ?"); params.append(feed)
    if set_speed is not None:
        op = _OP[set_speed_cmp]
        if set_speed_cmp == Cmp.eq:
            where.append("? BETWEEN set_speed_min AND set_speed_max")
            params.append(set_speed)
        else:
            col = "set_speed_max" if set_speed_cmp in (Cmp.gt, Cmp.gte) \
                else "set_speed_min"
            where.append(f"{col} {op} ?"); params.append(set_speed)

    def _range_filter(val, cmp, cmin, cmax):
        if val is None:
            return
        if cmp == Cmp.eq:
            where.append(f"? BETWEEN {cmin} AND {cmax}"); params.append(val)
        elif cmp in (Cmp.gt, Cmp.gte):
            where.append(f"{cmax} {_OP[cmp]} ?"); params.append(val)
        else:
            where.append(f"{cmin} {_OP[cmp]} ?"); params.append(val)

    _range_filter(vx, vx_cmp, "vx_min", "vx_max")
    _range_filter(vy, vy_cmp, "vy_min", "vy_max")
    _range_filter(water_valve, water_valve_cmp,
                  "water_valve_min", "water_valve_max")

    if require_channels:
        for ch in [c.strip() for c in require_channels.split(",") if c.strip()]:
            where.append("channels_present LIKE ?")
            params.append(f"%{ch}%")

    sql = "SELECT * FROM experiments"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY exp_date, run LIMIT ?"
    params.append(limit)

    con = _con()
    try:
        cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [_add_human_times(dict(zip(cols, r))) for r in cur.fetchall()]
    finally:
        con.close()
    return {"count": len(rows), "query": sql, "results": rows}


@app.get("/experiments/{experiment_id}/snapshot")
def get_snapshot(
    experiment_id: str,
    stream: str = Query("pump", pattern="^(pump|scan)$"),
    columns: str | None = Query(None, description="comma list; default all"),
    t_start: int | None = Query(None, description="ns, inclusive"),
    t_end: int | None = Query(None, description="ns, inclusive"),
    limit: int = 5000,
) -> dict[str, Any]:
    con = _con()
    try:
        row = con.execute(
            "SELECT pump_parquet, scan_parquet FROM experiments "
            "WHERE experiment_id = ?", [experiment_id]
        ).fetchone()
    finally:
        con.close()
    if not row:
        raise HTTPException(404, f"unknown experiment {experiment_id}")
    path = row[0] if stream == "pump" else row[1]
    if not path:
        # no data for this stream (e.g. a pump-only run) -> empty, not an error
        return {
            "experiment_id": experiment_id, "stream": stream,
            "rows_returned": 0, "columns": [], "data": [],
        }
    # stored paths are relative to the catalog's folder -> resolve them
    if not os.path.isabs(path):
        path = os.path.join(
            os.path.dirname(os.path.abspath(CATALOG_PATH)), path
        )

    sel = "*" if not columns else ",".join(
        c.strip() for c in columns.split(",") if c.strip()
    )
    q = f"SELECT {sel} FROM read_parquet(?)"
    params: list[Any] = [path]
    conds = []
    tcol = "timestamp" if stream == "pump" else "time"
    if t_start is not None:
        conds.append(f"{tcol} >= ?"); params.append(t_start)
    if t_end is not None:
        conds.append(f"{tcol} <= ?"); params.append(t_end)
    if conds:
        q += " WHERE " + " AND ".join(conds)
    q += " LIMIT ?"; params.append(limit)

    con = duckdb.connect(":memory:")
    try:
        cur = con.execute(q, params)
        cols = [d[0] for d in cur.description]
        data = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        con.close()
    return {
        "experiment_id": experiment_id, "stream": stream,
        "rows_returned": len(data), "columns": cols, "data": data,
    }


@app.get("/experiments/{experiment_id}")
def get_experiment(experiment_id: str) -> dict[str, Any]:
    con = _con()
    try:
        cur = con.execute(
            "SELECT * FROM experiments WHERE experiment_id = ?",
            [experiment_id],
        )
        cols = [d[0] for d in cur.description]
        r = cur.fetchone()
    finally:
        con.close()
    if not r:
        raise HTTPException(404, f"unknown experiment {experiment_id}")
    return _add_human_times(dict(zip(cols, r)))
