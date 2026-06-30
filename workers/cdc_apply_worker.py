"""Legacy entrypoint kept for compatibility.

The supported worker is ``worker.py``. Its CDC manager claims CDC migrations
from the PostgreSQL state DB and starts per-migration CDC apply threads.
"""

from __future__ import annotations

import sys


MESSAGE = """
cdc_apply_worker.py is no longer a supported entrypoint.

Run the universal worker instead:

    python worker.py

Docker already uses this entrypoint via Dockerfile.worker.
"""


def main() -> None:
    print(MESSAGE.strip(), file=sys.stderr)
    raise SystemExit(2)


if __name__ == "__main__":
    main()
