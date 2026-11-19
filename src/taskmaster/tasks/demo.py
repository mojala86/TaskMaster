"""Demo task types for exercising the pipeline end to end."""

import asyncio
import random
from typing import Any

from pydantic import BaseModel, Field

from taskmaster.tasks.registry import JobContext, task


class EchoInput(BaseModel):
    data: dict[str, Any] = Field(default_factory=dict)


@task("echo", max_retries=2, timeout=10)
async def echo(payload: EchoInput, ctx: JobContext) -> EchoInput:
    return payload


class SleepInput(BaseModel):
    seconds: float = Field(1.0, ge=0, le=300)


class SleepOutput(BaseModel):
    slept: float


@task("sleep", max_retries=2, timeout=310)
async def sleep(payload: SleepInput, ctx: JobContext) -> SleepOutput:
    await asyncio.sleep(payload.seconds)
    return SleepOutput(slept=payload.seconds)


class FlakyInput(BaseModel):
    fail_rate: float = Field(0.5, ge=0, le=1, description="Probability that an attempt fails")


class FlakyOutput(BaseModel):
    succeeded_on_attempt: int


@task("flaky", max_retries=3, timeout=10)
async def flaky(payload: FlakyInput, ctx: JobContext) -> FlakyOutput:
    """Fails randomly, to demonstrate retries with backoff and dead-lettering."""
    if random.random() < payload.fail_rate:
        raise RuntimeError(f"Simulated failure (attempt {ctx.attempt}/{ctx.max_attempts})")
    return FlakyOutput(succeeded_on_attempt=ctx.attempt)
