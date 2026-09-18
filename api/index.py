"""Vercel entry point for the FastAPI backend."""

import sys
from pathlib import Path

backend_dir = Path(__file__).resolve().parents[1] / "backend"
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from server import app  # noqa: E402

__all__ = ["app"]