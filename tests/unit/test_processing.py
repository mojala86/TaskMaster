"""Processing functions, run in-process against file:// URLs (no object storage needed)."""

import csv
from pathlib import Path

import pytest
from PIL import Image

from taskmaster.errors import PermanentError
from taskmaster.processing.csv_aggregate import aggregate_csv
from taskmaster.processing.image import resize_image


class Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[float, str | None]] = []

    def __call__(self, fraction: float, message: str | None = None) -> None:
        self.calls.append((fraction, message))


def write(path: Path, content: str | bytes) -> str:
    if isinstance(content, str):
        path.write_text(content, encoding="utf-8", newline="")
    else:
        path.write_bytes(content)
    return path.as_uri()


def read_csv(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


SALES = (
    "﻿region,product,amount,note\r\n"  # BOM + CRLF, as exported by Excel
    "north,a,10,ok\r\n"
    "south,b,5,\r\n"
    'north,b,2.5,"multi\nline note"\r\n'  # quoted newline inside a field
    "south,a,n/a,bad value\r\n"
    "north,a,7.5,ok\r\n"
    "short,row\r\n"
    "\r\n"
)


def test_csv_group_by_with_all_ops(tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    progress = Recorder()
    spec = {
        "group_by": ["region"],
        "aggregations": [
            {"op": "count"},
            {"op": "sum", "column": "amount"},
            {"op": "mean", "column": "amount"},
            {"op": "min", "column": "amount"},
            {"op": "max", "column": "amount"},
        ],
    }

    stats = aggregate_csv(write(tmp_path / "in.csv", SALES), str(out), spec, progress)

    assert stats == {"rows": 5, "groups": 2, "skipped_rows": 1, "invalid_values": 4}
    assert read_csv(out) == [
        ["region", "count", "sum_amount", "mean_amount", "min_amount", "max_amount"],
        ["north", "3", "20", "6.66666666666667", "2.5", "10"],
        ["south", "2", "5", "5", "5", "5"],
    ]
    assert progress.calls[-1][0] == 1.0


def test_csv_without_group_by_aggregates_everything(tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    spec = {"aggregations": [{"op": "sum", "column": "amount"}]}

    aggregate_csv(write(tmp_path / "in.csv", SALES), str(out), spec, Recorder())

    assert read_csv(out) == [["sum_amount"], ["25"]]


def test_csv_multiple_group_columns_and_delimiter(tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    data = "region;product;amount\nn;a;1\nn;a;2\nn;b;3\n"
    spec = {
        "group_by": ["region", "product"],
        "aggregations": [{"op": "sum", "column": "amount"}],
        "delimiter": ";",
    }

    aggregate_csv(write(tmp_path / "in.csv", data), str(out), spec, Recorder())

    assert read_csv(out) == [["region", "product", "sum_amount"], ["n", "a", "3"], ["n", "b", "3"]]


def test_csv_missing_column_is_permanent(tmp_path: Path) -> None:
    spec = {"group_by": ["country"], "aggregations": [{"op": "sum", "column": "price"}]}

    with pytest.raises(PermanentError, match=r"Columns not found: \['country', 'price'\]"):
        aggregate_csv(write(tmp_path / "in.csv", SALES), str(tmp_path / "o"), spec, Recorder())


def test_csv_empty_file_is_permanent(tmp_path: Path) -> None:
    spec = {"aggregations": [{"op": "count"}]}
    with pytest.raises(PermanentError, match="empty"):
        aggregate_csv(write(tmp_path / "in.csv", ""), str(tmp_path / "o"), spec, Recorder())


def test_csv_not_utf8_is_permanent(tmp_path: Path) -> None:
    spec = {"aggregations": [{"op": "count"}]}
    with pytest.raises(PermanentError, match="parse"):
        aggregate_csv(
            write(tmp_path / "in.csv", b"a,b\n\xff\xfe,1\n"), str(tmp_path / "o"), spec, Recorder()
        )


def make_png(path: Path, size: tuple[int, int], mode: str = "RGB") -> str:
    Image.new(mode, size, "red").save(path, format="PNG")
    return path.as_uri()


def test_image_fit_keeps_aspect_ratio(tmp_path: Path) -> None:
    out = tmp_path / "out.png"
    progress = Recorder()

    info = resize_image(
        make_png(tmp_path / "in.png", (640, 480)), str(out), {"width": 160}, progress
    )

    assert (info["width"], info["height"]) == (160, 120)
    assert (info["source_width"], info["source_height"]) == (640, 480)
    with Image.open(out) as img:
        assert img.size == (160, 120)
    assert [p for p, _ in progress.calls] == [0.3, 0.7, 1.0]


def test_image_fit_never_upscales(tmp_path: Path) -> None:
    info = resize_image(
        make_png(tmp_path / "in.png", (100, 50)),
        str(tmp_path / "out.png"),
        {"width": 1000, "height": 1000},
        Recorder(),
    )
    assert (info["width"], info["height"]) == (100, 50)


def test_image_exact_to_jpeg_drops_alpha(tmp_path: Path) -> None:
    out = tmp_path / "out.jpg"
    spec = {"width": 50, "height": 50, "mode": "exact", "format": "jpeg"}

    resize_image(make_png(tmp_path / "in.png", (640, 480), mode="RGBA"), str(out), spec, Recorder())

    with Image.open(out) as img:
        assert (img.format, img.mode, img.size) == ("JPEG", "RGB", (50, 50))


def test_not_an_image_is_permanent(tmp_path: Path) -> None:
    with pytest.raises(PermanentError, match="Not a supported image"):
        resize_image(
            write(tmp_path / "in.png", b"definitely not a png"),
            str(tmp_path / "o.png"),
            {"width": 10},
            Recorder(),
        )
