"""Auto-installed on subprocess startup when this directory is on
PYTHONPATH and ``COVERAGE_PROCESS_START`` points at a coverage config.
conftest sets both env vars when spawning the daemon — result: every
line the daemon executes shows up in ``pytest --cov`` reports."""

import os

if os.environ.get("COVERAGE_PROCESS_START"):  # pragma: no cover - boot hook
    try:
        import coverage

        coverage.process_startup()
    except ImportError:
        pass
