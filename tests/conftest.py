import asyncio
import sys

# psycopg's async mode needs a selector event loop; Windows defaults to Proactor.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined,unused-ignore]
