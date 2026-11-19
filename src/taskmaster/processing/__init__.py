"""CPU-bound processing that runs in a dedicated subprocess per job.

Modules here import only the standard library, Pillow and `taskmaster.errors`, so starting a
subprocess stays cheap. Functions take a source URL (a presigned GET URL, or file:// in tests),
a destination path and a `progress(fraction, message)` callback.
"""
