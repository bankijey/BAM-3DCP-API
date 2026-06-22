"""
Parse 3DCP experiment filenames into structured metadata.

Expected pattern (double-underscore separated tokens):
    2025-08-22__BOARD__F10000__S0__W40__run_0.csv

Token meaning (adjust DECODERS below to match your lab's convention):
    <date>     ISO date  YYYY-MM-DD
    <board>    specimen / substrate label (free text token)
    F<int>     feed rate / flow setpoint
    S<int>     setSpeed setpoint
    W<int>     nozzle / bead width
    run_<int>  repetition index

The parser is deliberately tolerant: unknown tokens are kept in `extra`
so a slightly different filename never breaks ingest (point 4: drift).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import date
from pathlib import Path
from typing import Any

# Map a single-letter prefix -> (field_name, caster)
DECODERS: dict[str, tuple[str, Any]] = {
    "F": ("feed", int),
    "S": ("set_speed", int),
    "W": ("width", int),
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RUN_RE = re.compile(r"^run[_-]?(\d+)$", re.IGNORECASE)


@dataclass
class ExperimentMeta:
    filename: str
    stem: str
    exp_date: date | None = None
    board: str | None = None
    feed: int | None = None
    set_speed: int | None = None
    width: int | None = None
    run: int | None = None
    extra: dict[str, str] = field(default_factory=dict)
    parse_ok: bool = True
    parse_warnings: list[str] = field(default_factory=list)

    def as_row(self) -> dict[str, Any]:
        d = asdict(self)
        d["exp_date"] = self.exp_date.isoformat() if self.exp_date else None
        d["extra"] = ";".join(f"{k}={v}" for k, v in self.extra.items())
        d["parse_warnings"] = ";".join(self.parse_warnings)
        return d


def parse_experiment_filename(path: str | Path) -> ExperimentMeta:
    p = Path(path)
    stem = p.stem
    meta = ExperimentMeta(filename=p.name, stem=stem)

    tokens = stem.split("__")
    if len(tokens) < 2:
        meta.parse_ok = False
        meta.parse_warnings.append("fewer than 2 tokens; non-standard filename")

    for i, tok in enumerate(tokens):
        if i == 0 and _DATE_RE.match(tok):
            y, m, d = map(int, tok.split("-"))
            try:
                meta.exp_date = date(y, m, d)
            except ValueError:
                meta.parse_warnings.append(f"bad date token: {tok}")
            continue

        run_m = _RUN_RE.match(tok)
        if run_m:
            meta.run = int(run_m.group(1))
            continue

        prefix = tok[:1]
        if prefix in DECODERS and tok[1:].lstrip("-").isdigit():
            fname, caster = DECODERS[prefix]
            try:
                setattr(meta, fname, caster(tok[1:]))
            except ValueError:
                meta.parse_warnings.append(f"could not cast token: {tok}")
            continue

        # First unclassified non-date token -> treat as board label.
        if meta.board is None and not tok[:1].isdigit():
            meta.board = tok
            continue

        meta.extra[f"tok{i}"] = tok

    if meta.parse_warnings:
        meta.parse_ok = meta.parse_ok and False
    return meta


if __name__ == "__main__":
    for name in [
        "2025-08-22__BOARD__F10000__S0__W40__run_0.csv",
        "2025-10-23__PLATE__F8000__S250__W20__run_3.json",
        "weird_old_file.csv",
    ]:
        m = parse_experiment_filename(name)
        print(name)
        for k, v in m.as_row().items():
            print(f"   {k:16} {v}")
        print()
