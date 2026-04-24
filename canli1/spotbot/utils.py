from __future__ import annotations

import json
import math
import os
import statistics
import time
from datetime import datetime, timezone
from typing import Any


UTC = timezone.utc


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def now_ts() -> float:
    return time.time()


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(value, hi))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def pct_change(new: float, old: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old * 100.0


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    try:
        return statistics.stdev(values)
    except Exception:
        return 0.0


def atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def load_json(path: str) -> dict[str, Any] | None:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_iso_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def round_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    units = math.floor(value / step)
    return round(units * step, 12)


def logistic(value: float) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-value))
    except OverflowError:
        return 0.0 if value < 0 else 1.0


def softmax_triplet(first: float, second: float, third: float) -> tuple[float, float, float]:
    max_value = max(first, second, third)
    exps = [math.exp(item - max_value) for item in (first, second, third)]
    total = sum(exps)
    if total <= 0:
        return (1 / 3, 1 / 3, 1 / 3)
    return tuple(item / total for item in exps)  # type: ignore[return-value]


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
