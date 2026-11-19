from typing import Any

import pytest
from pydantic import BaseModel

from taskmaster.tasks import JobContext, all_tasks, get_task, task


def test_demo_tasks_registered() -> None:
    assert {t.name for t in all_tasks()} >= {"echo", "sleep", "flaky"}


def test_definition_exposes_input_model_and_attempts() -> None:
    definition = get_task("flaky")
    assert definition is not None
    assert definition.max_attempts == definition.max_retries + 1
    assert "fail_rate" in definition.input_model.model_json_schema()["properties"]


def test_rejects_handler_without_model_annotation() -> None:
    with pytest.raises(TypeError, match="BaseModel"):

        @task("test.untyped")
        async def untyped(payload: dict[str, Any], ctx: JobContext) -> None: ...


def test_rejects_sync_handler() -> None:
    class Input(BaseModel):
        pass

    with pytest.raises(TypeError, match="async"):

        @task("test.sync")
        def sync(payload: Input, ctx: JobContext) -> None: ...  # type: ignore[arg-type]


def test_rejects_duplicate_name() -> None:
    with pytest.raises(ValueError, match="already registered"):

        @task("echo")
        async def echo_again(payload: BaseModel, ctx: JobContext) -> None: ...
