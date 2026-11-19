"""Streaming CSV group-by aggregation.

The file is read straight from the HTTP response through a csv.reader, one row at a time, so
memory grows with the number of distinct groups, never with the file size. Quoted fields that
contain newlines work because the csv module sees one continuous text stream.
"""

import csv
import io
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from taskmaster.errors import PermanentError
from taskmaster.processing.runner import Progress

OPS = ("count", "sum", "min", "max", "mean")
PROGRESS_EVERY_ROWS = 50_000


class _CountingReader(io.RawIOBase):
    """Raw byte stream that counts bytes read, for progress = bytes read / Content-Length."""

    def __init__(self, source: Any) -> None:
        self._source = source
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        n: int = self._source.readinto(buffer)
        self.bytes_read += n
        return n


@dataclass
class _ColumnStats:
    """Running sum/min/max/mean of the numeric values seen in one column of one group."""

    count: int = 0
    total: float = 0.0
    min: float | None = None
    max: float | None = None

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.min = value if self.min is None else min(self.min, value)
        self.max = value if self.max is None else max(self.max, value)

    def value(self, op: str) -> float | None:
        if self.count == 0:
            return None  # no numeric values in this group
        match op:
            case "sum":
                return self.total
            case "min":
                return self.min
            case "max":
                return self.max
            case "mean":
                return self.total / self.count
        raise ValueError(f"Unknown op {op!r}")


@dataclass
class _Group:
    rows: int = 0
    stats: list[_ColumnStats] = field(default_factory=list)

    def result(self, op: str, index: int) -> float | int | None:
        return self.rows if op == "count" else self.stats[index].value(op)


def aggregate_csv(
    source_url: str, dest_path: str, spec: dict[str, Any], progress: Progress
) -> dict[str, int]:
    group_by: list[str] = spec.get("group_by", [])
    aggregations: list[tuple[str | None, str]] = [
        (a.get("column"), a["op"]) for a in spec["aggregations"]
    ]
    delimiter: str = spec.get("delimiter", ",")

    try:
        with urllib.request.urlopen(source_url) as response:
            total_bytes = int(response.headers.get("Content-Length") or 0)
            counter = _CountingReader(response)
            text = io.TextIOWrapper(
                io.BufferedReader(counter, 1024 * 1024), encoding="utf-8-sig", newline=""
            )
            reader = csv.reader(text, delimiter=delimiter)

            header = next(reader, None)
            if header is None:
                raise PermanentError("CSV file is empty")
            index = {name.strip(): i for i, name in enumerate(header)}
            needed = group_by + [col for col, op in aggregations if op != "count" and col]
            missing = sorted({c for c in needed if c not in index})
            if missing:
                raise PermanentError(f"Columns not found: {missing}. Header: {header}")
            key_idx = [index[c] for c in group_by]
            agg_idx = [index[col] if col else -1 for col, _ in aggregations]

            groups: dict[tuple[str, ...], _Group] = {}
            rows = skipped_rows = invalid_values = 0
            for row in reader:
                if not row:
                    continue  # blank line
                if len(row) < len(header):
                    skipped_rows += 1
                    continue
                rows += 1
                key = tuple(row[i] for i in key_idx)
                group = groups.get(key)
                if group is None:
                    group = groups[key] = _Group(stats=[_ColumnStats() for _ in aggregations])
                group.rows += 1
                for stats, i, (_, op) in zip(group.stats, agg_idx, aggregations, strict=True):
                    if op == "count":
                        continue
                    try:
                        stats.add(float(row[i]))
                    except ValueError:
                        invalid_values += 1
                if rows % PROGRESS_EVERY_ROWS == 0 and total_bytes:
                    progress(counter.bytes_read / total_bytes, f"{rows:,} rows")
    except (csv.Error, UnicodeDecodeError) as exc:
        # Malformed input: retrying will not help.
        raise PermanentError(f"Could not parse CSV: {exc}") from exc

    with open(dest_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)
        writer.writerow(
            group_by + [op if op == "count" else f"{op}_{col}" for col, op in aggregations]
        )
        for key in sorted(groups):
            group = groups[key]
            values = [_fmt(group.result(op, i)) for i, (_, op) in enumerate(aggregations)]
            writer.writerow([*key, *values])

    progress(1.0, f"{rows:,} rows, {len(groups):,} groups")
    return {
        "rows": rows,
        "groups": len(groups),
        "skipped_rows": skipped_rows,
        "invalid_values": invalid_values,
    }


def _fmt(value: float | int | None) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    return f"{value:.15g}"
