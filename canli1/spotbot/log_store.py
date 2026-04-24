from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import asdict
from typing import Any

from .models import LogEntry, TradeRecord
from .utils import safe_float


_CREATE_LOGS = """
CREATE TABLE IF NOT EXISTS logs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      TEXT    NOT NULL,
    level   TEXT    NOT NULL,
    message TEXT    NOT NULL,
    extra   TEXT    NOT NULL DEFAULT '{}'
)
"""

_CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    trade_id   TEXT PRIMARY KEY,
    mode       TEXT NOT NULL,
    slot_slug  TEXT NOT NULL,
    side       TEXT NOT NULL,
    status     TEXT NOT NULL,
    pnl_usdc   REAL NOT NULL DEFAULT 0,
    entry_fee  REAL NOT NULL DEFAULT 0,
    exit_fee   REAL NOT NULL DEFAULT 0,
    opened_at  TEXT NOT NULL DEFAULT '',
    closed_at  TEXT NOT NULL DEFAULT ''
)
"""


class LogStore:
    """
    In-memory SQLite store for log entries and closed trade summaries.
    Provides fast SQL queries (hourly stats, level filters) without list scanning.
    Thread-safe via internal lock — safe to call while holding BotRuntime.lock.
    """

    def __init__(self, max_log_rows: int = 5000) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._lock = threading.Lock()
        self._max_log_rows = max_log_rows
        with self._lock:
            self._conn.executescript(f"""
                {_CREATE_LOGS};
                CREATE INDEX IF NOT EXISTS idx_log_ts ON logs(ts);
                {_CREATE_TRADES};
                CREATE INDEX IF NOT EXISTS idx_trade_closed ON trades(closed_at);
            """)
            self._conn.commit()

    # ── Log methods ──────────────────────────────────────────────────────────

    def insert_log(self, entry: LogEntry) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO logs (ts, level, message, extra) VALUES (?, ?, ?, ?)",
                (entry.ts, entry.level, entry.message, json.dumps(entry.extra, ensure_ascii=False)),
            )
            # Keep table bounded — delete oldest rows beyond max_log_rows
            self._conn.execute(
                "DELETE FROM logs WHERE id <= (SELECT MAX(id) - ? FROM logs)",
                (self._max_log_rows,),
            )
            self._conn.commit()

    def tail_logs(self, n: int = 200) -> list[LogEntry]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT ts, level, message, extra FROM logs ORDER BY id DESC LIMIT ?",
                (n,),
            ).fetchall()
        return [
            LogEntry(ts=r[0], level=r[1], message=r[2], extra=json.loads(r[3]))
            for r in reversed(rows)
        ]

    def log_dicts(self, n: int = 120) -> list[dict[str, Any]]:
        return [asdict(e) for e in self.tail_logs(n)]

    def restore_logs(self, rows: list[Any]) -> None:
        entries = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = str(row.get("ts") or "").strip()
            level = str(row.get("level") or "").strip()
            message = str(row.get("message") or "").strip()
            if ts and level and message:
                extra = row.get("extra", {})
                entries.append(LogEntry(
                    ts=ts, level=level, message=message,
                    extra=extra if isinstance(extra, dict) else {},
                ))
        if not entries:
            return
        with self._lock:
            self._conn.executemany(
                "INSERT OR IGNORE INTO logs (ts, level, message, extra) VALUES (?, ?, ?, ?)",
                [(e.ts, e.level, e.message, json.dumps(e.extra)) for e in entries],
            )
            self._conn.commit()

    # ── Trade methods ─────────────────────────────────────────────────────────

    def upsert_trade(self, trade: TradeRecord) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO trades (trade_id, mode, slot_slug, side, status,
                    pnl_usdc, entry_fee, exit_fee, opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    status    = excluded.status,
                    pnl_usdc  = excluded.pnl_usdc,
                    exit_fee  = excluded.exit_fee,
                    closed_at = excluded.closed_at
                """,
                (
                    trade.trade_id, trade.mode, trade.slot_slug, trade.side,
                    trade.status, round(trade.pnl_usdc, 8),
                    round(trade.entry_fee_usdc, 8), round(trade.exit_fee_usdc, 8),
                    trade.opened_at, trade.closed_at,
                ),
            )
            self._conn.commit()

    def restore_trades(self, rows: list[Any]) -> None:
        # rows are raw dicts from state file — store only summary columns
        entries = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            tid = str(row.get("trade_id") or "").strip()
            if not tid:
                continue
            entries.append((
                tid,
                str(row.get("mode") or "paper"),
                str(row.get("slot_slug") or ""),
                str(row.get("side") or ""),
                str(row.get("status") or "closed"),
                safe_float(row.get("pnl_usdc")),
                safe_float(row.get("entry_fee_usdc")),
                safe_float(row.get("exit_fee_usdc")),
                str(row.get("opened_at") or ""),
                str(row.get("closed_at") or ""),
            ))
        if not entries:
            return
        with self._lock:
            self._conn.executemany(
                """
                INSERT OR IGNORE INTO trades
                (trade_id, mode, slot_slug, side, status, pnl_usdc,
                 entry_fee, exit_fee, opened_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                entries,
            )
            self._conn.commit()

    def hourly_stats(self, since_iso: str) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    COUNT(*)                                          AS count,
                    SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END)   AS wins,
                    SUM(CASE WHEN pnl_usdc <= 0 THEN 1 ELSE 0 END)  AS losses,
                    COALESCE(SUM(pnl_usdc), 0)                       AS net_pnl,
                    COALESCE(SUM(CASE WHEN pnl_usdc > 0 THEN pnl_usdc ELSE 0 END), 0) AS total_won,
                    COALESCE(SUM(CASE WHEN pnl_usdc <= 0 THEN pnl_usdc ELSE 0 END), 0) AS total_lost
                FROM trades
                WHERE status IN ('closed', 'resolved') AND closed_at >= ?
                """,
                (since_iso,),
            ).fetchone()
        return {
            "count":      int(row[0] or 0),
            "win_count":  int(row[1] or 0),
            "loss_count": int(row[2] or 0),
            "net_pnl":    round(float(row[3] or 0), 4),
            "total_won":  round(float(row[4] or 0), 4),
            "total_lost": round(float(row[5] or 0), 4),
        }

    def total_stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    COUNT(*),
                    SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END),
                    COALESCE(SUM(pnl_usdc), 0)
                FROM trades
                WHERE status IN ('closed', 'resolved')
                """,
            ).fetchone()
        count = int(row[0] or 0)
        wins = int(row[1] or 0)
        pnl = round(float(row[2] or 0), 8)
        return {
            "count": count,
            "wins": wins,
            "losses": count - wins,
            "pnl": pnl,
            "win_rate_pct": round(wins / count * 100, 4) if count > 0 else 0.0,
        }

    def clear_trades(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM trades")
            self._conn.commit()

    def clear_logs(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM logs")
            self._conn.commit()
