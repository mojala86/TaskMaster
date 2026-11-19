from fastapi import APIRouter

from taskmaster.api.schemas import TaskInfo
from taskmaster.tasks import all_tasks

router = APIRouter(tags=["tasks"])


@router.get("/tasks")
async def list_tasks() -> list[TaskInfo]:
    """Registered task types with the JSON schema of their payload."""
    return [
        TaskInfo(
            name=t.name,
            max_retries=t.max_retries,
            timeout_seconds=t.timeout,
            input_schema=t.input_model.model_json_schema(),
        )
        for t in all_tasks()
    ]
