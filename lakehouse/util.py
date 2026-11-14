from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    value = value or now()
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_iso() -> str:
    return iso()


def epoch() -> float:
    return time.time()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def loads(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def human_remaining(expires_at: float | None) -> float:
    if not expires_at:
        return 0.0
    return max(0.0, round(expires_at - epoch(), 1))


def tail(text: str, limit: int = 2000) -> str:
    return text if len(text) <= limit else "..." + text[-limit:]
