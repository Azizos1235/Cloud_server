import json
import math
from datetime import date as Date, datetime, timezone
from typing import Optional


def num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def opt(v) -> Optional[float]:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def sid(v):
    return str(v) if v is not None else None


def age_months(birth: Date) -> int:
    t = datetime.now(timezone.utc).date()
    return (t.year - birth.year) * 12 + (t.month - birth.month) - (1 if t.day < birth.day else 0)


def load_payload(body: bytes) -> list:
    """Accept one object, a list of objects, or double-encoded JSON."""
    payload = json.loads(body)
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload if isinstance(payload, list) else [payload]


def parse_ts(value, default: datetime) -> datetime:
    """ISO-8601 string -> aware UTC datetime."""
    if isinstance(value, str):
        try:
            t = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return default
        return t.astimezone(timezone.utc) if t.tzinfo else t.replace(tzinfo=timezone.utc)
    return default
