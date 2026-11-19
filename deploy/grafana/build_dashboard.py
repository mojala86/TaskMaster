"""Generates dashboards/taskmaster.json. Run after changing panels:

uv run python deploy/grafana/build_dashboard.py
"""

import json
from pathlib import Path
from typing import Any

DATASOURCE = {"type": "prometheus", "uid": "prometheus"}
OUT = Path(__file__).parent / "dashboards" / "taskmaster.json"

_panels: list[dict[str, Any]] = []
_y = 0
_next_id = 1


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id


def row(title: str) -> None:
    global _y
    _panels.append(
        {
            "type": "row",
            "title": title,
            "id": _id(),
            "collapsed": False,
            "gridPos": {"x": 0, "y": _y, "w": 24, "h": 1},
            "panels": [],
        }
    )
    _y += 1


def panels(*specs: dict[str, Any], height: int = 8) -> None:
    """Lay out one row of panels side by side; widths are given per panel (sum to 24)."""
    global _y
    x = 0
    for spec in specs:
        width = spec.pop("w")
        kind = spec.pop("kind", "timeseries")
        unit = spec.pop("unit", "short")
        targets = [
            {"datasource": DATASOURCE, "expr": expr, "legendFormat": legend, "refId": chr(65 + i)}
            for i, (expr, legend) in enumerate(spec.pop("targets"))
        ]
        panel: dict[str, Any] = {
            "type": kind,
            "id": _id(),
            "datasource": DATASOURCE,
            "gridPos": {"x": x, "y": _y, "w": width, "h": height},
            "targets": targets,
            "fieldConfig": {
                "defaults": {"unit": unit, **spec.pop("defaults", {})},
                "overrides": [],
            },
            "options": spec.pop("options", {}),
            **spec,
        }
        if kind == "timeseries":
            panel["fieldConfig"]["defaults"].setdefault(
                "custom", {"lineWidth": 2, "fillOpacity": 10, "showPoints": "never"}
            )
            panel["options"].setdefault("legend", {"displayMode": "list", "placement": "bottom"})
            panel["options"].setdefault("tooltip", {"mode": "multi"})
        _panels.append(panel)
        x += width
    _y += height


def stat(title: str, expr: str, unit: str, w: int, thresholds: list[tuple[float, str]]) -> dict:
    return {
        "title": title,
        "kind": "stat",
        "w": w,
        "unit": unit,
        "targets": [(expr, "")],
        "defaults": {
            "thresholds": {
                "mode": "absolute",
                "steps": [{"color": c, "value": v} for v, c in thresholds],
            },
            "color": {"mode": "thresholds"},
        },
        "options": {
            "colorMode": "background",
            "graphMode": "area",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        },
    }


GREEN = [(None, "green")]  # type: ignore[list-item]

# --- Overview -----------------------------------------------------------------------------
row("Overview")
panels(
    stat("Submitted / s", "sum(rate(taskmaster_jobs_submitted_total[1m]))", "reqps", 4, GREEN),
    stat(
        "Finished / s",
        'sum(rate(taskmaster_job_runs_total{outcome=~"succeeded|dead|cancelled"}[1m]))',
        "reqps",
        4,
        GREEN,
    ),
    stat(
        "Error rate (5m)",
        '(sum(rate(taskmaster_job_runs_total{outcome=~"failed|dead"}[5m])) or vector(0))'
        " / clamp_min(sum(rate(taskmaster_job_runs_total[5m])), 1e-9)",
        "percentunit",
        4,
        [(None, "green"), (0.05, "orange"), (0.2, "red")],
    ),
    stat(
        "Waiting to run",
        'max(taskmaster_jobs{status=~"queued|retrying"})',
        "short",
        4,
        [(None, "green"), (100, "orange"), (1000, "red")],
    ),
    stat("Running", "sum(taskmaster_jobs_in_progress) or vector(0)", "short", 4, GREEN),
    stat(
        "Dead-letter stream",
        'max(taskmaster_stream_length{stream="taskmaster:dead-letter"})',
        "short",
        4,
        [(None, "green"), (1, "orange")],
    ),
    height=5,
)

# --- Throughput and latency -----------------------------------------------------------------
row("Jobs")
panels(
    {
        "title": "Job runs / s by outcome",
        "w": 12,
        "unit": "ops",
        "targets": [("sum by (outcome) (rate(taskmaster_job_runs_total[1m]))", "{{outcome}}")],
    },
    {
        "title": "Submitted / s by type",
        "w": 12,
        "unit": "ops",
        "targets": [("sum by (type) (rate(taskmaster_jobs_submitted_total[1m]))", "{{type}}")],
    },
)
panels(
    {
        "title": "Run time p50 / p95 by type",
        "w": 12,
        "unit": "s",
        "targets": [
            (
                "histogram_quantile(0.5, sum by (le, type) (rate(taskmaster_job_duration_seconds_bucket[5m])))",
                "p50 {{type}}",
            ),
            (
                "histogram_quantile(0.95, sum by (le, type) (rate(taskmaster_job_duration_seconds_bucket[5m])))",
                "p95 {{type}}",
            ),
        ],
    },
    {
        "title": "Queue wait (created → first start) p50 / p95",
        "w": 12,
        "unit": "s",
        "targets": [
            (
                "histogram_quantile(0.5, sum by (le) (rate(taskmaster_job_queue_wait_seconds_bucket[5m])))",
                "p50",
            ),
            (
                "histogram_quantile(0.95, sum by (le) (rate(taskmaster_job_queue_wait_seconds_bucket[5m])))",
                "p95",
            ),
        ],
    },
)

# --- Queue ----------------------------------------------------------------------------------
row("Queue")
panels(
    {
        "title": "Unfinished jobs by status",
        "w": 8,
        "targets": [("max by (status) (taskmaster_jobs)", "{{status}}")],
    },
    {
        "title": "Work stream: length and pending (unacked)",
        "w": 8,
        "targets": [
            ('max(taskmaster_stream_length{stream="taskmaster:jobs"})', "length"),
            ("max(taskmaster_stream_pending)", "pending"),
        ],
    },
    {
        "title": "Outbox: due events and lag",
        "w": 8,
        "targets": [
            ("max(taskmaster_outbox_due)", "due events"),
            ("max(taskmaster_outbox_lag_seconds)", "lag (s)"),
        ],
    },
)

# --- Reliability ----------------------------------------------------------------------------
row("Reliability")
panels(
    {
        "title": "Recovery actions (per 5m)",
        "w": 12,
        "targets": [
            (
                "sum(increase(taskmaster_leases_expired_total[5m])) or vector(0)",
                "leases expired (worker died)",
            ),
            (
                "sum(increase(taskmaster_messages_reclaimed_total[5m])) or vector(0)",
                "messages reclaimed",
            ),
            (
                "sum(increase(taskmaster_jobs_redispatched_total[5m])) or vector(0)",
                "jobs re-dispatched",
            ),
            (
                'sum(increase(taskmaster_job_runs_total{outcome="released"}[5m])) or vector(0)',
                "released on shutdown",
            ),
        ],
    },
    {"title": "Scrape targets up", "w": 12, "targets": [("count by (job) (up == 1)", "{{job}}")]},
)

# --- API ------------------------------------------------------------------------------------
row("API")
panels(
    {
        "title": "Requests / s by status",
        "w": 8,
        "unit": "reqps",
        "targets": [
            (
                'sum by (status) (rate(taskmaster_http_requests_total{route!="/metrics"}[1m]))',
                "{{status}}",
            )
        ],
    },
    {
        "title": "Latency p95 by route",
        "w": 8,
        "unit": "s",
        "targets": [
            (
                'histogram_quantile(0.95, sum by (le, route) (rate(taskmaster_http_request_duration_seconds_bucket{route!="/metrics"}[5m])))',
                "{{route}}",
            )
        ],
    },
    {
        "title": "Rate limited (429) / s",
        "w": 8,
        "unit": "reqps",
        "targets": [
            ("sum by (client) (rate(taskmaster_rate_limited_total[1m]))", "{{client}}"),
            ("sum(rate(taskmaster_rate_limiter_errors_total[1m]))", "limiter errors (fail open)"),
        ],
    },
)

dashboard = {
    "uid": "taskmaster",
    "title": "TaskMaster",
    "tags": ["taskmaster"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": _panels,
    "templating": {"list": []},
    "annotations": {"list": []},
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} ({len(_panels)} panels)")
