"""Legacy entrypoint kept for compatibility.

The supported worker is ``worker.py``. It claims bulk, baseline, CDC,
data-compare, and DDL-apply work directly from the PostgreSQL state DB.
"""

from __future__ import annotations

import sys


MESSAGE = """
bulk_worker.py is no longer a supported entrypoint.

Run the universal worker instead:

    python worker.py

Docker already uses this entrypoint via Dockerfile.worker.
"""


def main() -> None:
    print(MESSAGE.strip(), file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":
    main()
