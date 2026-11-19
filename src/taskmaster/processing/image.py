"""Image resizing with Pillow."""

import io
import urllib.request
from typing import Any

from PIL import Image, ImageOps, UnidentifiedImageError

from taskmaster.errors import PermanentError
from taskmaster.processing.runner import Progress

MAX_INPUT_BYTES = 100 * 1024 * 1024


def resize_image(
    source_url: str, dest_path: str, spec: dict[str, Any], progress: Progress
) -> dict[str, Any]:
    """Resize to fit within width x height (keeping aspect ratio, never upscaling) or to the exact
    size. Pillow decodes the whole image anyway, so the input is read into memory, with a cap."""
    with urllib.request.urlopen(source_url) as response:
        size = int(response.headers.get("Content-Length") or 0)
        if size > MAX_INPUT_BYTES:
            raise PermanentError(f"Image is {size:,} bytes; the limit is {MAX_INPUT_BYTES:,}")
        data = response.read(MAX_INPUT_BYTES + 1)
    if len(data) > MAX_INPUT_BYTES:
        raise PermanentError(f"Image exceeds the {MAX_INPUT_BYTES:,} byte limit")
    progress(0.3, "downloaded")

    try:
        image: Image.Image = Image.open(io.BytesIO(data))
        image.load()
    except (UnidentifiedImageError, Image.DecompressionBombError) as exc:
        raise PermanentError(f"Not a supported image: {exc}") from exc
    source_width, source_height = image.size
    image = ImageOps.exif_transpose(image)  # respect camera orientation

    width: int = spec["width"]
    height: int | None = spec.get("height")
    if spec.get("mode", "fit") == "exact":
        assert height is not None
        image = image.resize((width, height), Image.Resampling.LANCZOS)
    else:
        image.thumbnail((width, height or 1_000_000), Image.Resampling.LANCZOS)
    progress(0.7, "resized")

    fmt: str = spec.get("format", "png")
    options: dict[str, Any] = {"optimize": True}
    if fmt in ("jpeg", "webp"):
        options["quality"] = spec.get("quality", 85)
    if fmt == "jpeg" and image.mode not in ("RGB", "L"):
        image = image.convert("RGB")  # JPEG has no alpha channel
    image.save(dest_path, format=fmt.upper(), **options)
    progress(1.0, "saved")

    return {
        "source_width": source_width,
        "source_height": source_height,
        "width": image.width,
        "height": image.height,
        "format": fmt,
    }
