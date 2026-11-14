from __future__ import annotations

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_DIR = os.path.join(ROOT, "var", "lake")
DEFAULT_DB_PATH = os.path.join(ROOT, "var", "lakehouse.sqlite3")
DEFAULT_LEASE_TTL = 90.0
MAX_COMMIT_ATTEMPTS = 8
MAX_LOOP_ITERATIONS = 24
ROW_CAP = 5000
