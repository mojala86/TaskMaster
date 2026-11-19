"""File-processing task types.

- file.checksum:  async streaming in the worker process (I/O bound).
- csv.aggregate:  streaming group-by in a subprocess (CPU bound).
- image.resize:   Pillow in a subprocess (CPU bound).
"""

import hashlib
from typing import Literal, Self

from pydantic import BaseModel, Field, model_validator

from taskmaster.processing.csv_aggregate import aggregate_csv
from taskmaster.processing.image import resize_image
from taskmaster.tasks.context import JobContext
from taskmaster.tasks.registry import task
from taskmaster.tasks.types import FileRef

# --- file.checksum ------------------------------------------------------------------------


class ChecksumInput(BaseModel):
    file: FileRef
    algorithm: Literal["sha256", "sha1", "md5"] = "sha256"


class ChecksumOutput(BaseModel):
    algorithm: str
    digest: str
    bytes: int


@task("file.checksum", max_retries=2, timeout=1800)
async def checksum(payload: ChecksumInput, ctx: JobContext) -> ChecksumOutput:
    size = (await ctx.input_info(payload.file)).size
    digest = hashlib.new(payload.algorithm)
    done = 0
    async for chunk in ctx.stream_input(payload.file):
        digest.update(chunk)
        done += len(chunk)
        ctx.report_progress(done / size if size else 1.0, f"{done:,} / {size:,} bytes")
    return ChecksumOutput(algorithm=payload.algorithm, digest=digest.hexdigest(), bytes=done)


# --- csv.aggregate ------------------------------------------------------------------------


class Aggregation(BaseModel):
    op: Literal["count", "sum", "min", "max", "mean"]
    column: str | None = Field(None, description="Required for every op except count")

    @model_validator(mode="after")
    def _column_required(self) -> Self:
        if self.op != "count" and not self.column:
            raise ValueError(f"'{self.op}' needs a column")
        return self


class CsvAggregateInput(BaseModel):
    file: FileRef
    group_by: list[str] = Field(default_factory=list, max_length=10)
    aggregations: list[Aggregation] = Field(min_length=1, max_length=50)
    delimiter: str = Field(",", min_length=1, max_length=1)


class CsvAggregateOutput(BaseModel):
    rows: int
    groups: int
    skipped_rows: int = Field(description="Rows with fewer columns than the header")
    invalid_values: int = Field(description="Values that were not numbers (ignored)")
    output: str


@task("csv.aggregate", max_retries=2, timeout=3600)
async def csv_aggregate(payload: CsvAggregateInput, ctx: JobContext) -> CsvAggregateOutput:
    out = ctx.workdir / "aggregate.csv"
    stats = await ctx.run_cpu(
        aggregate_csv, await ctx.input_url(payload.file), str(out), payload.model_dump()
    )
    await ctx.save_output("aggregate.csv", out, "text/csv")
    return CsvAggregateOutput(**stats, output="aggregate.csv")


# --- image.resize -------------------------------------------------------------------------


class ImageResizeInput(BaseModel):
    file: FileRef
    width: int = Field(ge=1, le=10_000)
    height: int | None = Field(None, ge=1, le=10_000)
    mode: Literal["fit", "exact"] = Field(
        "fit", description="fit: within width x height, keep aspect, never upscale"
    )
    format: Literal["png", "jpeg", "webp"] = "png"
    quality: int = Field(85, ge=1, le=100)

    @model_validator(mode="after")
    def _exact_needs_height(self) -> Self:
        if self.mode == "exact" and self.height is None:
            raise ValueError("mode 'exact' needs a height")
        return self


class ImageResizeOutput(BaseModel):
    source_width: int
    source_height: int
    width: int
    height: int
    format: str
    output: str


@task("image.resize", max_retries=2, timeout=300)
async def image_resize(payload: ImageResizeInput, ctx: JobContext) -> ImageResizeOutput:
    name = f"resized.{'jpg' if payload.format == 'jpeg' else payload.format}"
    out = ctx.workdir / name
    info = await ctx.run_cpu(
        resize_image, await ctx.input_url(payload.file), str(out), payload.model_dump()
    )
    await ctx.save_output(name, out, f"image/{payload.format}")
    return ImageResizeOutput(**info, output=name)
