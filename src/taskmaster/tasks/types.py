"""Field types for task input models."""

import re
from typing import Annotated, Any

from pydantic import BaseModel, StringConstraints

UPLOAD_KEY_PATTERN = r"^uploads/[0-9a-f-]{36}/[A-Za-z0-9._-]{1,255}$"
_UPLOAD_KEY = re.compile(UPLOAD_KEY_PATTERN)


class _FileRefMarker:
    """Marks a field as a reference to an uploaded file (checked to exist at submission)."""

    def __repr__(self) -> str:
        return "FileRef"


FILE_REF = _FileRefMarker()

# Key of a file uploaded through POST /uploads, e.g. "uploads/<uuid>/sales.csv".
FileRef = Annotated[str, StringConstraints(pattern=UPLOAD_KEY_PATTERN), FILE_REF]


def file_refs(payload: BaseModel) -> list[str]:
    """Values of the top-level FileRef fields of a validated payload."""
    keys: list[str] = []
    for name, field in type(payload).model_fields.items():
        if any(m is FILE_REF for m in field.metadata):
            value: Any = getattr(payload, name)
            if isinstance(value, str):
                keys.append(value)
    return keys


def is_upload_key(key: str) -> bool:
    return bool(_UPLOAD_KEY.match(key))
