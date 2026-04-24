from __future__ import annotations

import threading
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from importlib import metadata as importlib_metadata
from typing import Any

from .log_store import LogStore
from .models import BotConfig, LogEntry, MarketSnapshot, MarketSide, PredictionSnapshot, TradeRecord
from .polymarket import BinanceBtcClient, PolymarketLiveClient, PolymarketPublicClient, SignalEngine
from .secrets import SecretStore
from .telegram import TelegramNotifier
from .utils import atomic_write_json, coerce_bool, load_json, parse_iso_ts, safe_float, utc_now_iso
from .ws_feeds import BinanceWsFeeder, PolymarketWsFeeder


UTC = timezone.utc
STATE_SCHEMA = "polymarket_btc_5m_v1"


class BotRuntime:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

        self.secret_store = SecretStore(config.secrets_file)
        self.secrets = self.secret_store.load()
        self.notifier = TelegramNotifier(
            token=self.secrets.telegram_bot_token,
            chat_id=self.secrets.telegram_chat_id,
            base_url=self.secrets.telegram_api_base_url,
            timeout=config.request_timeout_sec,
        )
        self.telegram_command_stop = threading.Event()
        self.telegram_command_thread: threading.Thread | None = None
        self.telegram_update_offset: int | None = None
        self.telegram_command_last_error = ""

        self.public_client = PolymarketPublicClient(config)
        self.btc_client = BinanceBtcClient(config.price_source_base_url, timeout=config.request_timeout_sec)
        self.signal_engine = SignalEngine(config)
        self.live_client = self._build_live_client()
        self.binance_ws = BinanceWsFeeder()
        self.poly_ws = PolymarketWsFeeder()
        self._last_ws_token_ids: list[str] = []

        self.log_store = LogStore(max_log_rows=5000)
        self.paper_cash_usdc = config.paper_balance_usdc
        self.active_trade: TradeRecord | None = None
        self.pending_resolution_trades: list[TradeRecord] = []
        self.closed_trades: list[TradeRecord] = []
        self.recent_cycles: list[dict[str, Any]] = []
        self.last_signal: PredictionSnapshot | None = None
        self.last_market: MarketSnapshot | None = None
        self.last_live_shadow: dict[str, Any] = {}
        self.last_paper_shadow_skip_notify_key = ""
        self.last_market_shadow_notify_slug = ""
        self.pending_shadow_market_reports: list[dict[str, Any]] = []
        self.shadow_comparison_history: list[dict[str, Any]] = []
        self.last_geoblock: dict[str, Any] = {}
        self.last_collateral_status: dict[str, Any] = {}
        self.last_live_positions: list[dict[str, Any]] = []
        self.last_traded_slot_slug = ""
        self.cycle_index = 0
        self.last_cycle_started_at = ""
        self.last_cycle_finished_at = ""
        self.last_error: str | None = None
        self.live_rollout_started_at = ""
        self.last_effective_mode = "paper"
        self.last_hourly_report_key = ""
        self.last_hourly_report_sent_at = ""
        self.pnl_tracking_started_at = utc_now_iso()
        self.live_suspended_reason = ""
        self.live_suspended_at = ""
        self.live_order_failures = 0
        self.live_confirmation_slot = ""
        self.live_confirmation_direction = ""
        self.live_confirmation_count = 0
        self.live_sdk_package = self._detect_live_sdk_package()
        self._cycle_live_shadow_cache_key = ""
        self._cycle_live_shadow_cache_result: dict[str, Any] = {}
        self._last_persist_monotonic = 0.0
        self._persist_interval_seconds = 4.0

        self._restore_state()
        self.public_client = PolymarketPublicClient(config)
        self.btc_client = BinanceBtcClient(config.price_source_base_url, timeout=config.request_timeout_sec)
        self.signal_engine = SignalEngine(config)
        self.live_client = self._build_live_client()
        self.notifier.timeout = config.request_timeout_sec
        self._ensure_rollout_anchor()
        self._reset_live_confirmation()
        self.binance_ws.start()
        self.poly_ws.start()
        self.last_effective_mode = self._effective_mode()
        self._log(
            "INFO",
            "Polymarket BTC 5m runtime hazır",
            target_mode=self.config.trading_mode,
            effective_mode=self.last_effective_mode,
            paper_grace_minutes=self.config.paper_grace_minutes,
        )
        self._start_telegram_commands()

    def _build_live_client(self) -> PolymarketLiveClient | None:
        if not self.secrets.has_polymarket_live:
            return None
        return PolymarketLiveClient(
            self.config,
            private_key=self.secrets.polymarket_private_key,
            funder_address=self.secrets.polymarket_funder_address,
            signature_type=self.secrets.polymarket_signature_type,
        )

    def _overlay_ws_prices(self, market: MarketSnapshot) -> MarketSnapshot:
        up_ws = self.poly_ws.get_price(market.up.token_id)
        down_ws = self.poly_ws.get_price(market.down.token_id)
        max_age = float(self.config.polymarket_ws_max_age_seconds)
        now = time.time()
        if (
            up_ws
            and up_ws.get("bid", 0) > 0
            and up_ws.get("ask", 0) > 0
            and now - safe_float(up_ws.get("updated_ts")) <= max_age
        ):
            market.up.bid = up_ws["bid"]
            market.up.ask = up_ws["ask"]
            market.up.price = up_ws["price"] or market.up.price
            market.best_bid = market.up.bid
            market.best_ask = market.up.ask
        if (
            down_ws
            and down_ws.get("bid", 0) > 0
            and down_ws.get("ask", 0) > 0
            and now - safe_float(down_ws.get("updated_ts")) <= max_age
        ):
            market.down.bid = down_ws["bid"]
            market.down.ask = down_ws["ask"]
            market.down.price = down_ws["price"] or market.down.price
        return market

    def _feed_status(self, market: MarketSnapshot | None = None) -> dict[str, Any]:
        token_ids: list[str] = []
        if market:
            token_ids = [item for item in (market.up.token_id, market.down.token_id) if item]
        return {
            "binance": self.binance_ws.status(),
            "polymarket": self.poly_ws.status(token_ids),
        }

    def _live_data_guard_reason(self, signal: PredictionSnapshot, market: MarketSnapshot) -> str:
        if not self.binance_ws.is_fresh(float(self.config.binance_ws_max_age_seconds)):
            return "live_binance_ws_stale"
        if not self.config.live_reprice_before_order:
            side = market.side(signal.direction)
            if not self.poly_ws.is_price_fresh(side.token_id, float(self.config.polymarket_ws_max_age_seconds)):
                return "live_polymarket_ws_stale"
        return ""

    def _signal_max_entry_price(self, signal: PredictionSnapshot) -> float:
        base = float(self.config.max_entry_price)
        top = max(base, float(getattr(self.config, "momentum_max_entry_price", base)))
        value = safe_float(signal.features.get("signal_max_entry_price"))
        if value > 0:
            return round(max(base, min(value, top)), 4)
        return round(base, 4)

    def _live_entry_limit_price(self, ask: float, signal: PredictionSnapshot) -> float:
        max_entry = self._signal_max_entry_price(signal)
        return min(max_entry, round(float(ask) + float(self.config.live_entry_price_buffer), 4))

    def _fresh_live_market_for_order(
        self,
        market: MarketSnapshot,
        signal: PredictionSnapshot,
        amount_usdc: float,
    ) -> MarketSnapshot:
        if not self.config.live_reprice_before_order:
            return market
        fresh = self.public_client.get_clob_market(market.condition_id)
        if fresh is None:
            raise RuntimeError("live_reprice_market_unavailable")
        if not fresh.slug:
            fresh.slug = market.slug
        if not fresh.question:
            fresh.question = market.question
        if market.start_time:
            fresh.start_time = market.start_time
        if market.end_time:
            fresh.end_time = market.end_time
        original_side = market.side(signal.direction)
        fresh_side = fresh.side(signal.direction)
        if fresh_side.token_id and original_side.token_id and fresh_side.token_id != original_side.token_id:
            raise RuntimeError("live_reprice_token_mismatch")
        if not fresh_side.token_id:
            fresh_side.token_id = original_side.token_id
        max_entry_price = self._signal_max_entry_price(signal)
        signal.features["signal_max_entry_price_effective"] = round(max_entry_price, 4)
        quote = self.public_client.get_order_book_quote(fresh_side.token_id)
        if safe_float(quote.get("bid")) > 0:
            fresh_side.bid = safe_float(quote.get("bid"))
        if safe_float(quote.get("ask")) > 0:
            fresh_side.ask = safe_float(quote.get("ask"))
        if safe_float(quote.get("mid")) > 0:
            fresh_side.price = safe_float(quote.get("mid"))
        spread = max(0.0, round(fresh_side.ask - max(0.0, fresh_side.bid), 6))
        signal.features["live_bid"] = round(fresh_side.bid, 4)
        signal.features["live_ask"] = round(fresh_side.ask, 4)
        signal.features["live_spread"] = spread
        if fresh.closed or not fresh.accepting_orders:
            raise RuntimeError("live_reprice_market_closed")
        if fresh_side.ask <= 0:
            raise RuntimeError("live_reprice_ask_missing")
        if fresh_side.ask > max_entry_price:
            raise RuntimeError("live_reprice_price_too_high")
        limit_price = self._live_entry_limit_price(fresh_side.ask, signal)
        signal.features["live_limit_price"] = round(limit_price, 4)
        signal.features["live_order_type_fok"] = 1.0
        if limit_price < fresh_side.ask:
            raise RuntimeError("live_reprice_limit_below_ask")
        if limit_price > safe_float(quote.get("depth_limit_price")) + 1e-9:
            quote = self.public_client.get_order_book_quote(fresh_side.token_id, limit_price)
        book_age = safe_float(quote.get("book_age_seconds"), 999999.0)
        required_depth = round(float(amount_usdc) * float(self.config.live_orderbook_depth_multiplier), 6)
        ask_depth = safe_float(quote.get("ask_depth_usdc"))
        signal.features["live_orderbook_depth_usdc"] = round(ask_depth, 6)
        signal.features["live_orderbook_required_usdc"] = required_depth
        signal.features["live_orderbook_age_seconds"] = round(book_age, 3)
        if book_age > float(self.config.live_orderbook_max_age_seconds):
            raise RuntimeError("live_reprice_book_stale")
        if ask_depth < required_depth:
            raise RuntimeError("live_reprice_depth_too_low")
        if spread > float(self.config.live_max_spread):
            raise RuntimeError("live_reprice_spread_too_wide")
        end_ts = parse_iso_ts(fresh.end_time)
        if end_ts:
            seconds_to_close = end_ts - datetime.now(UTC).timestamp()
            signal.features["live_seconds_to_close"] = round(seconds_to_close, 3)
            if seconds_to_close < int(self.config.min_seconds_left_to_trade):
                raise RuntimeError("live_reprice_too_late")
        return fresh

    @staticmethod
    def _side_from_dict(payload: dict[str, Any]) -> Any:
        from .models import MarketSide

        return MarketSide(
            label=str(payload.get("label", "")),
            token_id=str(payload.get("token_id", "")),
            price=safe_float(payload.get("price")),
            bid=safe_float(payload.get("bid")),
            ask=safe_float(payload.get("ask")),
        )

    def _market_from_dict(self, payload: dict[str, Any]) -> MarketSnapshot:
        up_payload = payload.get("up", {}) if isinstance(payload.get("up"), dict) else {}
        down_payload = payload.get("down", {}) if isinstance(payload.get("down"), dict) else {}
        return MarketSnapshot(
            slug=str(payload.get("slug", "")),
            question=str(payload.get("question", "")),
            condition_id=str(payload.get("condition_id", "")),
            start_time=str(payload.get("start_time", "")),
            end_time=str(payload.get("end_time", "")),
            active=bool(payload.get("active", False)),
            closed=bool(payload.get("closed", False)),
            accepting_orders=bool(payload.get("accepting_orders", False)),
            restricted=bool(payload.get("restricted", False)),
            tick_size=safe_float(payload.get("tick_size"), 0.01),
            min_order_size=safe_float(payload.get("min_order_size"), 5.0),
            spread=safe_float(payload.get("spread")),
            best_bid=safe_float(payload.get("best_bid")),
            best_ask=safe_float(payload.get("best_ask")),
            last_trade_price=safe_float(payload.get("last_trade_price")),
            volume=safe_float(payload.get("volume")),
            liquidity=safe_float(payload.get("liquidity")),
            fees_enabled=bool(payload.get("fees_enabled", True)),
            fee_rate=safe_float(payload.get("fee_rate")),
            updated_at=str(payload.get("updated_at", "")),
            up=self._side_from_dict(up_payload),
            down=self._side_from_dict(down_payload),
            resolved_outcome=str(payload.get("resolved_outcome", "")),
        )

    def _trade_from_dict(self, payload: dict[str, Any]) -> TradeRecord | None:
        if not isinstance(payload, dict):
            return None
        trade_id = str(payload.get("trade_id", "") or "").strip()
        slot_slug = str(payload.get("slot_slug", "") or "").strip()
        side = str(payload.get("side", "") or "").strip().upper()
        if not trade_id or not slot_slug or side not in {"UP", "DOWN"}:
            return None
        return TradeRecord(
            trade_id=trade_id,
            mode=str(payload.get("mode", "paper") or "paper"),
            slot_slug=slot_slug,
            question=str(payload.get("question", "") or ""),
            condition_id=str(payload.get("condition_id", "") or ""),
            side=side,
            token_id=str(payload.get("token_id", "") or ""),
            end_time=str(payload.get("end_time", "") or ""),
            shares=safe_float(payload.get("shares")),
            status=str(payload.get("status", "open") or "open"),
            entry_price=safe_float(payload.get("entry_price")),
            entry_notional_usdc=safe_float(payload.get("entry_notional_usdc")),
            entry_fee_usdc=safe_float(payload.get("entry_fee_usdc")),
            exit_price=safe_float(payload.get("exit_price")),
            exit_notional_usdc=safe_float(payload.get("exit_notional_usdc")),
            exit_fee_usdc=safe_float(payload.get("exit_fee_usdc")),
            pnl_usdc=safe_float(payload.get("pnl_usdc")),
            pnl_pct=safe_float(payload.get("pnl_pct")),
            opened_at=str(payload.get("opened_at", "") or ""),
            closed_at=str(payload.get("closed_at", "") or ""),
            exit_reason=str(payload.get("exit_reason", "") or ""),
            resolution_outcome=str(payload.get("resolution_outcome", "") or ""),
            entry_order_id=str(payload.get("entry_order_id", "") or ""),
            exit_order_id=str(payload.get("exit_order_id", "") or ""),
            live_order_status=str(payload.get("live_order_status", "") or ""),
            synced_size=bool(payload.get("synced_size", False)),
            redeemable=bool(payload.get("redeemable", False)),
            notes=[str(item) for item in payload.get("notes", []) if str(item or "").strip()] if isinstance(payload.get("notes"), list) else [],
        )

    def _signal_from_dict(self, payload: dict[str, Any]) -> PredictionSnapshot | None:
        if not isinstance(payload, dict):
            return None
        slug = str(payload.get("slug", "") or "").strip()
        direction = str(payload.get("direction", "") or "").strip().upper()
        if not slug or direction not in {"UP", "DOWN"}:
            return None
        features = payload.get("features", {})
        return PredictionSnapshot(
            slug=slug,
            direction=direction,
            confidence=safe_float(payload.get("confidence"), 50.0),
            fair_up_price=safe_float(payload.get("fair_up_price"), 0.5),
            fair_down_price=safe_float(payload.get("fair_down_price"), 0.5),
            market_price=safe_float(payload.get("market_price")),
            market_bid=safe_float(payload.get("market_bid")),
            market_ask=safe_float(payload.get("market_ask")),
            expected_edge=safe_float(payload.get("expected_edge")),
            strength_score=safe_float(
                payload.get("strength_score"),
                safe_float(payload.get("confidence"), 50.0) + max(0.0, safe_float(payload.get("expected_edge"))) * 100.0,
            ),
            score=safe_float(payload.get("score")),
            seconds_into_slot=int(safe_float(payload.get("seconds_into_slot"))),
            seconds_to_close=int(safe_float(payload.get("seconds_to_close"))),
            entry_allowed=bool(payload.get("entry_allowed", False)),
            reason=str(payload.get("reason", "") or ""),
            created_at=str(payload.get("created_at", "") or ""),
            btc_price=safe_float(payload.get("btc_price")),
            features={str(key): safe_float(value) for key, value in features.items()} if isinstance(features, dict) else {},
        )

    def _restore_state(self) -> None:
        payload = load_json(self.config.state_file) or {}
        if not isinstance(payload, dict):
            return
        schema = str(payload.get("schema", "") or "").strip()
        legacy_payload = any(key in payload for key in ("meta", "system", "portfolio", "summary")) and "active_trade" not in payload
        if schema and schema != STATE_SCHEMA:
            self._log("WARNING", "Bilinmeyen state schema yok sayıldı", schema=schema)
            return
        if not schema and legacy_payload:
            self._log("INFO", "Eski futures state yok sayıldı", file=self.config.state_file)
            return
        saved_config = payload.get("config", {})
        if isinstance(saved_config, dict):
            for name, value in saved_config.items():
                if name in {"state_file", "secrets_file"}:
                    continue
                if hasattr(self.config, name):
                    setattr(self.config, name, value)
        self.paper_cash_usdc = safe_float(payload.get("paper_cash_usdc"), self.config.paper_balance_usdc)
        trade_row = payload.get("active_trade", {})
        if isinstance(trade_row, dict):
            self.active_trade = self._trade_from_dict(trade_row)
        self.pending_resolution_trades = []
        pending_rows = payload.get("pending_resolution_trades", [])
        for row in pending_rows:
            trade = self._trade_from_dict(row) if isinstance(row, dict) else None
            if trade is not None and not any(item.trade_id == trade.trade_id for item in self.pending_resolution_trades):
                self.pending_resolution_trades.append(trade)
        if self.active_trade and self.active_trade.status in {"pending_resolution", "redeemable"}:
            if not any(item.trade_id == self.active_trade.trade_id for item in self.pending_resolution_trades):
                self.pending_resolution_trades.append(self.active_trade)
            self.active_trade = None
        self.closed_trades = []
        closed_rows = payload.get("closed_trades", [])
        for row in closed_rows:
            trade = self._trade_from_dict(row) if isinstance(row, dict) else None
            if trade is not None:
                self.closed_trades.append(trade)
        self.log_store.restore_logs(payload.get("logs", []))
        self.log_store.restore_trades(closed_rows)
        self.recent_cycles = [row for row in payload.get("recent_cycles", []) if isinstance(row, dict)]
        signal_row = payload.get("last_signal")
        market_row = payload.get("last_market")
        if isinstance(signal_row, dict):
            self.last_signal = self._signal_from_dict(signal_row)
        if isinstance(market_row, dict) and market_row.get("slug"):
            try:
                self.last_market = self._market_from_dict(market_row)
            except Exception:
                self.last_market = None
        self.last_live_shadow = payload.get("last_live_shadow", {}) if isinstance(payload.get("last_live_shadow"), dict) else {}
        self.last_geoblock = payload.get("last_geoblock", {}) if isinstance(payload.get("last_geoblock"), dict) else {}
        self.last_collateral_status = (
            payload.get("last_collateral_status", {}) if isinstance(payload.get("last_collateral_status"), dict) else {}
        )
        self.last_live_positions = [item for item in payload.get("last_live_positions", []) if isinstance(item, dict)] if isinstance(payload.get("last_live_positions"), list) else []
        self.last_traded_slot_slug = str(payload.get("last_traded_slot_slug", "") or "")
        self.last_market_shadow_notify_slug = str(payload.get("last_market_shadow_notify_slug", "") or "")
        self.pending_shadow_market_reports = [
            item for item in payload.get("pending_shadow_market_reports", []) if isinstance(item, dict)
        ] if isinstance(payload.get("pending_shadow_market_reports"), list) else []
        self.shadow_comparison_history = [
            item for item in payload.get("shadow_comparison_history", []) if isinstance(item, dict)
        ] if isinstance(payload.get("shadow_comparison_history"), list) else []
        self.cycle_index = int(safe_float(payload.get("cycle_index"), 0.0))
        self.last_cycle_started_at = str(payload.get("last_cycle_started_at", "") or "")
        self.last_cycle_finished_at = str(payload.get("last_cycle_finished_at", "") or "")
        self.live_rollout_started_at = str(payload.get("live_rollout_started_at", "") or "")
        self.last_hourly_report_key = str(payload.get("last_hourly_report_key", "") or "")
        self.last_hourly_report_sent_at = str(payload.get("last_hourly_report_sent_at", "") or "")
        self.live_suspended_reason = str(payload.get("live_suspended_reason", "") or "")
        self.live_suspended_at = str(payload.get("live_suspended_at", "") or "")
        self.live_order_failures = int(safe_float(payload.get("live_order_failures"), 0.0))
        self.live_confirmation_slot = str(payload.get("live_confirmation_slot", "") or "")
        self.live_confirmation_direction = str(payload.get("live_confirmation_direction", "") or "")
        self.live_confirmation_count = int(safe_float(payload.get("live_confirmation_count"), 0.0))
        self.pnl_tracking_started_at = str(payload.get("pnl_tracking_started_at", "") or self.pnl_tracking_started_at)

    def _persist_state(self) -> None:
        atomic_write_json(
            self.config.state_file,
            {
                "schema": STATE_SCHEMA,
                "saved_at": utc_now_iso(),
                "config": asdict(self.config),
                "paper_cash_usdc": round(self.paper_cash_usdc, 8),
                "active_trade": asdict(self.active_trade) if self.active_trade else None,
                "pending_resolution_trades": [asdict(item) for item in self.pending_resolution_trades[-20:]],
                "closed_trades": [asdict(item) for item in self.closed_trades[-40:]],
                "logs": self.log_store.log_dicts(120),
                "recent_cycles": self.recent_cycles[-40:],
                "last_signal": asdict(self.last_signal) if self.last_signal else None,
                "last_market": asdict(self.last_market) if self.last_market else None,
                "last_live_shadow": self.last_live_shadow,
                "last_geoblock": self.last_geoblock,
                "last_collateral_status": self.last_collateral_status,
                "last_live_positions": self.last_live_positions[-20:],
                "last_traded_slot_slug": self.last_traded_slot_slug,
                "last_market_shadow_notify_slug": self.last_market_shadow_notify_slug,
                "pending_shadow_market_reports": self.pending_shadow_market_reports[-40:],
                "shadow_comparison_history": self.shadow_comparison_history[-40:],
                "cycle_index": self.cycle_index,
                "last_cycle_started_at": self.last_cycle_started_at,
                "last_cycle_finished_at": self.last_cycle_finished_at,
                "live_rollout_started_at": self.live_rollout_started_at,
                "last_hourly_report_key": self.last_hourly_report_key,
                "last_hourly_report_sent_at": self.last_hourly_report_sent_at,
                "pnl_tracking_started_at": self.pnl_tracking_started_at,
                "live_suspended_reason": self.live_suspended_reason,
                "live_suspended_at": self.live_suspended_at,
                "live_order_failures": int(self.live_order_failures),
                "live_confirmation_slot": self.live_confirmation_slot,
                "live_confirmation_direction": self.live_confirmation_direction,
                "live_confirmation_count": int(self.live_confirmation_count),
            },
        )
        self._last_persist_monotonic = time.monotonic()

    def _persist_state_if_due(self, *, force: bool = False) -> None:
        if force or (time.monotonic() - self._last_persist_monotonic) >= self._persist_interval_seconds:
            self._persist_state()

    def _persistence_signature(self) -> tuple[Any, ...]:
        active_trade_id = self.active_trade.trade_id if self.active_trade else ""
        active_trade_status = self.active_trade.status if self.active_trade else ""
        pending = tuple((item.trade_id, item.status) for item in self.pending_resolution_trades[-10:])
        return (
            round(self.paper_cash_usdc, 8),
            active_trade_id,
            active_trade_status,
            pending,
            len(self.closed_trades),
            self.last_traded_slot_slug,
        )

    def _log(self, level: str, message: str, **extra: Any) -> None:
        self.log_store.insert_log(LogEntry(ts=utc_now_iso(), level=level.upper(), message=message, extra=extra))

    def _ensure_rollout_anchor(self) -> None:
        if self.config.trading_mode == "live":
            if not self.live_rollout_started_at:
                self.live_rollout_started_at = utc_now_iso()
        else:
            self.live_rollout_started_at = ""

    def _paper_grace_remaining_seconds(self, now: datetime | None = None) -> int:
        if self.config.trading_mode != "live":
            return 0
        grace_minutes = max(0, int(self.config.paper_grace_minutes))
        if grace_minutes <= 0:
            return 0
        self._ensure_rollout_anchor()
        anchor_ts = parse_iso_ts(self.live_rollout_started_at)
        if anchor_ts is None:
            self.live_rollout_started_at = utc_now_iso()
            anchor_ts = parse_iso_ts(self.live_rollout_started_at)
        if anchor_ts is None:
            return grace_minutes * 60
        current_ts = (now or datetime.now(UTC)).timestamp()
        return max(0, int(anchor_ts + grace_minutes * 60 - current_ts))

    def _effective_mode(self, now: datetime | None = None) -> str:
        if self.config.trading_mode != "live":
            return "paper"
        if self._paper_grace_remaining_seconds(now) > 0:
            return "paper"
        if self.active_trade and self.active_trade.mode == "live":
            return "live"
        if self.live_suspended_reason or self._clob_v2_guard_reason(now):
            return "paper"
        return "live"

    def _rollout_status(self, now: datetime | None = None) -> str:
        if self.config.trading_mode != "live":
            return "paper_only"
        remaining = self._paper_grace_remaining_seconds(now)
        if remaining > 0:
            minutes_left = max(1, (remaining + 59) // 60)
            return f"paper_guard:{minutes_left}m_left"
        if not self.live_client or not self.live_client.configured:
            return "live_blocked:secret_missing"
        guard_reason = self._clob_v2_guard_reason(now)
        if guard_reason:
            return f"live_blocked:{guard_reason}"
        if self.live_suspended_reason:
            return f"live_suspended:{self.live_suspended_reason}"
        if self.last_geoblock.get("blocked"):
            return "live_blocked:geoblocked"
        if self._live_collateral_balance() < self._max_order_size_usdc():
            return "live_blocked:collateral_low"
        return "live_active"

    def _maybe_log_mode_transition(self) -> str:
        effective_mode = self._effective_mode()
        if effective_mode != self.last_effective_mode:
            self.last_effective_mode = effective_mode
            self._log(
                "INFO",
                "Efektif mod değişti",
                effective_mode=effective_mode,
                target_mode=self.config.trading_mode,
                paper_grace_remaining_seconds=self._paper_grace_remaining_seconds(),
            )
            if effective_mode == "live":
                self._reset_live_confirmation()
                self._notify("Paper grace süresi bitti. Bot artık live emir açmayı deneyecek.")
        return effective_mode

    @staticmethod
    def _fmt_local_time(iso_str: str) -> str:
        try:
            return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone().strftime("%H:%M")
        except Exception:
            return "?"

    def _notify(self, message: str, *, respect_trade_toggle: bool = True) -> bool:
        if not self.notifier.configured:
            return False
        if respect_trade_toggle and not self.config.telegram_trade_notifications:
            return False
        try:
            self.notifier.send_message(message)
            return True
        except Exception as exc:
            self._log("ERROR", "Telegram bildirimi gonderilemedi", error=str(exc))
            return False

    @staticmethod
    def _telegram_commands_payload() -> list[dict[str, str]]:
        return [
            {"command": "baslat", "description": "Botu başlat"},
            {"command": "durdur", "description": "Botu durdur"},
            {"command": "yeniden_baslat", "description": "Botu durdurup yeniden başlat"},
            {"command": "rapor", "description": "Son 5 saatin kısa özeti"},
            {"command": "bakiye", "description": "Net bakiye ve başlangıçtan kazanç"},
            {"command": "durum", "description": "Botun anlık durumu"},
        ]

    def _start_telegram_commands(self) -> bool:
        if not self.notifier.configured:
            return False
        if self.telegram_command_thread and self.telegram_command_thread.is_alive():
            return True
        self.telegram_command_stop.clear()
        self.telegram_command_thread = threading.Thread(
            target=self._telegram_command_loop,
            name="telegram-command-listener",
            daemon=True,
        )
        self.telegram_command_thread.start()
        self._log("INFO", "Telegram komut dinleyici başlatıldı")
        return True

    @staticmethod
    def _normalize_telegram_command_token(value: str) -> str:
        token = str(value or "").strip().lower().lstrip("/")
        if "@" in token:
            token = token.split("@", 1)[0]
        replacements = {
            "ı": "i",
            "ğ": "g",
            "ü": "u",
            "ş": "s",
            "ö": "o",
            "ç": "c",
            "-": "_",
        }
        for source, target in replacements.items():
            token = token.replace(source, target)
        return "".join(ch for ch in token if ch.isalnum() or ch == "_")

    def _telegram_command_from_text(self, text: str) -> str:
        parts = str(text or "").strip().split()
        if not parts:
            return ""
        tokens = [self._normalize_telegram_command_token(part) for part in parts[:3]]
        if tokens[0] == "yeniden" and len(tokens) > 1 and tokens[1] in {"baslat", "start"}:
            return "yeniden_baslat"
        aliases = {
            "baslat": "baslat",
            "start": "baslat",
            "run": "baslat",
            "durdur": "durdur",
            "stop": "durdur",
            "pause": "durdur",
            "yeniden_baslat": "yeniden_baslat",
            "yenidenbaslat": "yeniden_baslat",
            "restart": "yeniden_baslat",
            "rapor": "rapor",
            "report": "rapor",
            "ozet": "rapor",
            "summary": "rapor",
            "bakiye": "bakiye",
            "balance": "bakiye",
            "pnl": "bakiye",
            "durum": "durum",
            "status": "durum",
            "state": "durum",
            "yardim": "yardim",
            "help": "yardim",
        }
        return aliases.get(tokens[0], "")

    @staticmethod
    def _mask_chat_id(chat_id: str) -> str:
        value = str(chat_id or "")
        if len(value) <= 4:
            return value
        return f"{value[:2]}...{value[-2:]}"

    def _telegram_chat_allowed(self, chat_id: str) -> bool:
        return str(chat_id or "").strip() == str(self.notifier.chat_id or "").strip()

    def _telegram_command_loop(self) -> None:
        commands_registered = False
        drained_pending = False
        while not self.telegram_command_stop.is_set():
            if not self.notifier.configured:
                commands_registered = False
                drained_pending = False
                self.telegram_command_stop.wait(3)
                continue
            try:
                if not commands_registered:
                    self.notifier.set_commands(self._telegram_commands_payload())
                    commands_registered = True
                    self.telegram_command_last_error = ""
                if not drained_pending and self.telegram_update_offset is None:
                    updates = self.notifier.get_updates(None, timeout_seconds=1)
                    if updates:
                        self.telegram_update_offset = max(int(item.get("update_id", 0) or 0) for item in updates) + 1
                    drained_pending = True
                    continue
                updates = self.notifier.get_updates(self.telegram_update_offset, timeout_seconds=20)
                for update in updates:
                    update_id = int(update.get("update_id", 0) or 0)
                    self.telegram_update_offset = max(self.telegram_update_offset or 0, update_id + 1)
                    self._handle_telegram_update(update)
                self.telegram_command_last_error = ""
            except Exception as exc:
                error = str(exc)
                if error != self.telegram_command_last_error:
                    self._log("ERROR", "Telegram komut dinleyici hata verdi", error=error)
                self.telegram_command_last_error = error
                self.telegram_command_stop.wait(5)

    def _handle_telegram_update(self, update: dict[str, Any]) -> None:
        message = update.get("message", {}) if isinstance(update, dict) else {}
        if not isinstance(message, dict):
            return
        chat = message.get("chat", {})
        chat_id = str(chat.get("id", "") if isinstance(chat, dict) else "")
        text = str(message.get("text", "") or "").strip()
        if not text:
            return
        if not self._telegram_chat_allowed(chat_id):
            if self._telegram_command_from_text(text):
                self._log("WARNING", "Yetkisiz Telegram komutu yok sayıldı", chat_id=self._mask_chat_id(chat_id))
            return
        self._handle_telegram_command(text)

    def _handle_telegram_command(self, text: str) -> None:
        command = self._telegram_command_from_text(text)
        if not command:
            if str(text or "").strip().startswith("/"):
                self._notify(self._format_telegram_help(), respect_trade_toggle=False)
            return
        if command == "baslat":
            result = self.start()
            with self.lock:
                status = self._format_status_report(compact=True)
            self._notify(f"✅ {result['message']}\n{status}", respect_trade_toggle=False)
            return
        if command == "durdur":
            result = self.stop()
            with self.lock:
                status = self._format_status_report(compact=True)
            self._notify(f"⏸️ {result['message']}\n{status}", respect_trade_toggle=False)
            return
        if command == "yeniden_baslat":
            self.stop()
            result = self.start()
            with self.lock:
                status = self._format_status_report(compact=True)
            self._notify(f"🔁 {result['message']}\n{status}", respect_trade_toggle=False)
            return
        with self.lock:
            if command == "rapor":
                reply = self._format_recent_report(hours=5)
            elif command == "bakiye":
                reply = self._format_balance_report()
            elif command == "durum":
                reply = self._format_status_report()
            else:
                reply = self._format_telegram_help()
        self._notify(reply, respect_trade_toggle=False)

    @staticmethod
    def _fmt_seconds(seconds: int | float) -> str:
        total = max(0, int(seconds))
        days, rem = divmod(total, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)
        if days:
            return f"{days}g {hours}s {minutes}dk"
        if hours:
            return f"{hours}s {minutes}dk"
        if minutes:
            return f"{minutes}dk {secs}sn"
        return f"{secs}sn"

    @staticmethod
    def _fmt_local_datetime(iso_str: str) -> str:
        try:
            return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).astimezone().strftime("%d.%m %H:%M")
        except Exception:
            return "?"

    def _closed_trades_since(self, since_ts: float) -> list[TradeRecord]:
        return [
            trade
            for trade in self.closed_trades
            if trade.status in {"closed", "resolved"} and (parse_iso_ts(trade.closed_at) or 0.0) >= since_ts
        ]

    def _polymarket_market_label(self, market: MarketSnapshot) -> str:
        question = str(market.question or "").strip()
        if question:
            return question if len(question) <= 96 else f"{question[:93]}..."
        return self._slot_window_label(market.slug, market.end_time)

    @staticmethod
    def _trade_note_value(trade: TradeRecord, key: str) -> str:
        prefix = f"{key}="
        for note in trade.notes:
            if note.startswith(prefix):
                return note[len(prefix):]
        return ""

    def _live_shadow_entry_check(
        self,
        market: MarketSnapshot,
        signal: PredictionSnapshot,
        amount_usdc: float,
    ) -> dict[str, Any]:
        cache_key = (
            f"{self.cycle_index}:{market.slug}:{signal.direction}:"
            f"{round(float(amount_usdc), 8)}:{signal.created_at}"
        )
        if self._cycle_live_shadow_cache_key == cache_key and self._cycle_live_shadow_cache_result:
            cached = dict(self._cycle_live_shadow_cache_result)
            self.last_live_shadow = cached
            return cached
        result: dict[str, Any] = {
            "checked_at": utc_now_iso(),
            "slot_slug": market.slug,
            "direction": signal.direction,
            "amount_usdc": round(float(amount_usdc), 8),
            "would_open": False,
            "reason": "not_checked",
            "max_entry_price": self._signal_max_entry_price(signal),
            "max_spread": round(float(self.config.live_max_spread), 4),
            "max_book_age_seconds": round(float(self.config.live_orderbook_max_age_seconds), 3),
            "min_seconds_left_to_trade": int(self.config.min_seconds_left_to_trade),
        }
        try:
            guard_reason = self._live_data_guard_reason(signal, market)
            if guard_reason:
                raise RuntimeError(guard_reason)
            fresh = self._fresh_live_market_for_order(market, signal, amount_usdc)
            side = fresh.side(signal.direction)
            limit_price = self._live_entry_limit_price(side.ask, signal)
            spread = max(0.0, round(side.ask - max(0.0, side.bid), 6))
            result.update(
                {
                    "would_open": True,
                    "reason": "ok",
                    "bid": round(side.bid, 4),
                    "ask": round(side.ask, 4),
                    "spread": spread,
                    "limit_price": round(limit_price, 4),
                    "book_age_seconds": safe_float(signal.features.get("live_orderbook_age_seconds")),
                    "book_depth_usdc": safe_float(signal.features.get("live_orderbook_depth_usdc")),
                    "required_depth_usdc": safe_float(signal.features.get("live_orderbook_required_usdc")),
                    "seconds_to_close": safe_float(signal.features.get("live_seconds_to_close"), -1.0),
                    "order_type": "FOK",
                }
            )
        except Exception as exc:
            result.update(
                {
                    "would_open": False,
                    "reason": str(exc) or exc.__class__.__name__,
                    "bid": safe_float(signal.features.get("live_bid")),
                    "ask": safe_float(signal.features.get("live_ask")),
                    "spread": safe_float(signal.features.get("live_spread")),
                    "book_age_seconds": safe_float(signal.features.get("live_orderbook_age_seconds"), -1.0),
                    "book_depth_usdc": safe_float(signal.features.get("live_orderbook_depth_usdc")),
                    "required_depth_usdc": safe_float(signal.features.get("live_orderbook_required_usdc")),
                    "limit_price": safe_float(signal.features.get("live_limit_price")),
                    "seconds_to_close": safe_float(signal.features.get("live_seconds_to_close"), -1.0),
                    "order_type": "FOK",
                }
            )
        self._cycle_live_shadow_cache_key = cache_key
        self._cycle_live_shadow_cache_result = dict(result)
        self.last_live_shadow = result
        self._log(
            "INFO",
            "Live shadow kontrolü",
            would_open=bool(result.get("would_open")),
            reason=result.get("reason"),
            slot=market.slug,
            direction=signal.direction,
            ask=result.get("ask"),
            limit_price=result.get("limit_price"),
            book_depth_usdc=result.get("book_depth_usdc"),
            required_depth_usdc=result.get("required_depth_usdc"),
            book_age_seconds=result.get("book_age_seconds"),
        )
        return result

    @staticmethod
    def _live_shadow_reason_text(result: dict[str, Any]) -> str:
        if result.get("would_open"):
            return "Canlı FOK emir açılabilirdi; veri taze, ask limit içinde, spread ve derinlik uygun."
        reason = str(result.get("reason") or "unknown")
        ask = safe_float(result.get("ask"))
        max_entry = safe_float(result.get("max_entry_price"))
        spread = safe_float(result.get("spread"))
        max_spread = safe_float(result.get("max_spread"))
        depth = safe_float(result.get("book_depth_usdc"))
        required = safe_float(result.get("required_depth_usdc"))
        age = safe_float(result.get("book_age_seconds"), -1.0)
        max_age = safe_float(result.get("max_book_age_seconds"))
        seconds_left = safe_float(result.get("seconds_to_close"), -1.0)
        min_seconds = safe_float(result.get("min_seconds_left_to_trade"))
        if reason == "ok":
            return "Canlı FOK emir açılabilirdi; tüm canlı giriş kontrolleri geçti."
        if reason == "live_binance_ws_stale":
            return "Binance fiyat akışı taze değil; sinyal fiyatı canlı emir için güvenilir sayılmadı."
        if reason == "live_polymarket_ws_stale":
            return "Polymarket token fiyat akışı taze değil; canlı emir öncesi fiyat güvenilir sayılmadı."
        if reason == "live_reprice_market_unavailable":
            return "Canlı emirden hemen önce market yeniden alınamadı."
        if reason == "live_reprice_token_mismatch":
            return "Canlı market token bilgisi beklenen token ile eşleşmedi."
        if reason == "live_reprice_market_closed":
            return "Market kapalı ya da emir kabul etmiyor."
        if reason == "live_reprice_ask_missing":
            return "Canlı orderbook ask fiyatı yok; alınacak fiyat bulunamadı."
        if reason == "live_reprice_price_too_high":
            return f"Canlı ask max girişin üstünde; ask {ask:.3f}, izin verilen max {max_entry:.3f}."
        if reason == "live_reprice_limit_below_ask":
            return "Canlı limit fiyatı ask altında kalıyor; FOK emir dolmazdı."
        if reason == "live_reprice_book_stale":
            return f"Orderbook verisi eski; yaş {age:.2f}sn, izin verilen max {max_age:.2f}sn."
        if reason == "live_reprice_depth_too_low":
            return f"Limit fiyatına kadar yeterli satış derinliği yok; ${depth:.2f} var, ${required:.2f} gerekiyor."
        if reason == "live_reprice_spread_too_wide":
            return f"Spread fazla geniş; spread {spread:.3f}, izin verilen max {max_spread:.3f}."
        if reason == "live_reprice_too_late":
            return f"Kapanışa çok az süre kaldı; {seconds_left:.0f}sn var, minimum {min_seconds:.0f}sn isteniyor."
        return reason

    @staticmethod
    def _format_live_shadow_result(result: dict[str, Any]) -> str:
        if not result:
            return "Gölge mod: henüz yok"
        status = "GEÇERDİ" if result.get("would_open") else "GEÇMEZDİ"
        reason = BotRuntime._live_shadow_reason_text(result)
        bid = safe_float(result.get("bid"))
        ask = safe_float(result.get("ask"))
        spread = safe_float(result.get("spread"))
        depth = safe_float(result.get("book_depth_usdc"))
        required = safe_float(result.get("required_depth_usdc"))
        age = safe_float(result.get("book_age_seconds"), -1.0)
        limit_price = safe_float(result.get("limit_price"))
        parts = [f"Gölge mod: {status}", f"Neden: {reason}"]
        if bid > 0 or ask > 0:
            bid_text = f"{bid:.3f}" if bid > 0 else "-"
            ask_text = f"{ask:.3f}" if ask > 0 else "-"
            parts.append(f"Bid/Ask: {bid_text}/{ask_text}")
        if limit_price > 0:
            parts.append(f"Limit: {limit_price:.3f}")
        if spread > 0:
            parts.append(f"Spread: {spread:.3f}")
        if depth > 0 or required > 0:
            parts.append(f"Book: ${depth:.2f}/${required:.2f}")
        if age >= 0:
            parts.append(f"Yaş: {age:.2f}sn")
        return " | ".join(parts)

    @staticmethod
    def _entry_reason_text(reason: str) -> str:
        clean = str(reason or "").strip()
        mapping = {
            "ok": "Giriş sinyali uygun.",
            "waiting_peak_window": "Bot giriş penceresini bekliyor.",
            "waiting_intraslot_momentum": "Slot ici BTC momentumu henuz yeterince guclenmedi.",
            "intraslot_momentum_too_small": "Slot ici BTC hareketi islem acmak icin cok zayif.",
            "entry_window_passed": "Bu markette giriş penceresi geçti.",
            "confidence_too_low": "Sinyal güveni minimum eşiğin altında.",
            "market_already_repriced": "Polymarket secilen yonu buyuk olcude fiyatlamis gorunuyor.",
            "market_too_expensive": "Seçilen yönün ask fiyatı sinyale izin verilen giriş tavanının üstünde.",
            "edge_too_small": "Beklenen edge minimum eşiğin altında.",
            "edge_too_low": "Beklenen edge minimum eşiğin altında.",
            "intraslot_reference_missing": "Slot ici referans BTC fiyati hazir degil.",
            "open_trade_exists": "Zaten açık işlem var.",
            "active_trade_exists": "Aktif işlem durumu devam ediyor.",
            "waiting_resolution": "Önceki işlem resmi sonuç bekliyor.",
            "slot_already_traded": "Bu markette daha önce işlem denendi/açıldı.",
            "insufficient_paper_cash": "Paper bakiye yetersiz.",
            "insufficient_collateral": "Canlı cüzdan bakiyesi yetersiz.",
            "polymarket_live_secret_missing": "Canlı Polymarket API bilgileri eksik.",
            "geoblocked": "Polymarket erişimi bölgesel olarak engelli görünüyor.",
            "live_binance_ws_stale": "Binance canlı veri akışı taze değil.",
            "live_polymarket_ws_stale": "Polymarket canlı veri akışı taze değil.",
            "waiting_live_confirmation": "Live için ek sinyal onayı bekleniyor.",
        }
        if clean.startswith("paper_live_shadow_failed:"):
            return "Giriş sinyali uygundu fakat gölge mod canlı emrin açılmayacağını gösterdi."
        if clean.startswith("live_suspended:"):
            return f"Live koruma kilidi aktif: {clean.split(':', 1)[1]}"
        return mapping.get(clean, clean or "?")

    def _format_market_shadow_report(
        self,
        market: MarketSnapshot,
        signal: PredictionSnapshot,
        shadow_result: dict[str, Any],
        entry_allowed: bool,
        entry_reason: str,
    ) -> str:
        local_now = datetime.now(UTC).astimezone().strftime("%H:%M:%S")
        slot_str = self._slot_window_label(market.slug, market.end_time)
        poly_market_str = self._polymarket_market_label(market)
        amount = safe_float(shadow_result.get("amount_usdc"), self._order_size_for_signal(signal))
        if entry_allowed and shadow_result.get("would_open"):
            gate_text = "Giriş sinyali uygun; gölge mod da geçti."
        elif entry_allowed:
            gate_text = "Giriş sinyali uygun ama gölge mod geçmediği için emir açılmayacak."
        else:
            gate_text = self._entry_reason_text(entry_reason)
        return (
            f"🟡 GÖLGE MOD — Market raporu\n"
            f"Bot saati: {local_now} | Yerel slot: {slot_str}\n"
            f"Polymarket: {poly_market_str}\n"
            f"Yön: {signal.direction} | Güven: %{signal.confidence:.1f} | Edge: {signal.expected_edge:.3f}\n"
            f"Tutar: ${amount:.2f} | İşlem kapısı: {gate_text}\n"
            f"{self._format_live_shadow_result(shadow_result)}"
        )

    @staticmethod
    def _gate_verdict_text(passed: bool, signal_correct: bool) -> str:
        if passed and signal_correct:
            return "Doğru: kazanan yön içeri alınmış olurdu."
        if passed and not signal_correct:
            return "Yanlış: kaybeden yön içeri alınmış olurdu."
        if not passed and signal_correct:
            return "Yanlış: kazanan yön kaçtı."
        return "Doğru: kaybeden yön filtrelendi."

    def _shadow_adjustment_hint(self, record: dict[str, Any], signal_correct: bool) -> str:
        entry_reason = str(record.get("entry_reason") or "")
        shadow_result = record.get("shadow_result", {}) if isinstance(record.get("shadow_result"), dict) else {}
        shadow_reason = str(shadow_result.get("reason") or "")
        shadow_would_open = bool(shadow_result.get("would_open"))
        entry_allowed = bool(record.get("entry_allowed"))
        if signal_correct:
            for reason in (entry_reason, shadow_reason):
                if reason == "confidence_too_low":
                    return "Ayar notu: min_confidence 1 puan kadar gevşetilerek tekrar izlenebilir."
                if reason in {"waiting_peak_window", "waiting_intraslot_momentum", "entry_window_passed", "live_reprice_too_late"}:
                    return "Ayar notu: giriş zaman penceresi hafif erkene çekilip tekrar ölçülebilir."
                if reason in {"market_too_expensive", "market_already_repriced", "live_reprice_price_too_high"}:
                    return "Ayar notu: max_entry_price ancak dikkatle artırılmalı; risk/ödül hızlı bozulur."
                if reason == "live_reprice_depth_too_low":
                    return "Ayar notu: order_size küçültmek veya derinlik tamponunu hafif gevşetmek daha güvenli olur."
                if reason == "live_reprice_spread_too_wide":
                    return "Ayar notu: spread filtresi gevşetilecekse önce daha fazla örnek biriktirmek iyi olur."
            if not entry_allowed or not shadow_would_open:
                return "Ayar notu: bu markette filtre fırsat kaçırmış olabilir; benzer örnekler birikirse ilgili eşik gevşetilebilir."
            return "Ayar notu: bu markette mevcut ayarlar yeterli görünüyor."
        if entry_allowed or shadow_would_open:
            return "Ayar notu: yanlış işlemleri azaltmak için min_confidence, min_edge veya live confirmation sıkılaştırılabilir."
        return "Ayar notu: bu markette filtreler loss tarafını engellemiş görünüyor; aynı sebebi hemen gevşetmek riskli olabilir."

    def _format_shadow_comparison_report(self, record: dict[str, Any], outcome: str) -> str:
        shadow_result = record.get("shadow_result", {}) if isinstance(record.get("shadow_result"), dict) else {}
        signal_correct = str(record.get("direction") or "").upper() == str(outcome or "").upper()
        entry_allowed = bool(record.get("entry_allowed"))
        shadow_would_open = bool(shadow_result.get("would_open"))
        slot_str = self._slot_window_label(str(record.get("slot_slug") or ""), str(record.get("end_time") or ""))
        result_emoji = "✅" if signal_correct else "❌"
        outcome_emoji = "🟢" if signal_correct else "🔴"
        return (
            f"🧪 SHADOW KIYAS — Market kapandı\n"
            f"Saat Araligi: {slot_str}\n"
            f"Yön: {record.get('direction')} | Güven: %{safe_float(record.get('confidence')):.1f} | Edge: {safe_float(record.get('edge')):.3f}\n"
            f"Gerçek sonuç: {outcome} {result_emoji}\n"
            f"İşlem kapısı: {'GEÇTİ' if entry_allowed else 'GEÇMEDİ'} | Neden: {self._entry_reason_text(str(record.get('entry_reason') or ''))}\n"
            f"İşlem kapısı kıyası: {self._gate_verdict_text(entry_allowed, signal_correct)}\n"
            f"{self._format_live_shadow_result(shadow_result)}\n"
            f"Gölge kıyası: {self._gate_verdict_text(shadow_would_open, signal_correct)}\n"
            f"{self._shadow_adjustment_hint(record, signal_correct)} {outcome_emoji}"
        )

    def _shadow_market_snapshot(self, record: dict[str, Any], current_market: MarketSnapshot | None) -> MarketSnapshot | None:
        slot_slug = str(record.get("slot_slug") or "")
        if current_market and current_market.slug == slot_slug:
            return current_market
        return self.public_client.get_market_by_slug(slot_slug)

    @staticmethod
    def _shadow_market_overdue_seconds(record: dict[str, Any]) -> int:
        end_ts = parse_iso_ts(str(record.get("end_time") or ""))
        if end_ts is None:
            return 0
        return int(datetime.now(UTC).timestamp() - end_ts)

    def _official_resolution_market_for_shadow(self, record: dict[str, Any]) -> MarketSnapshot | None:
        condition_id = str(record.get("condition_id") or "")
        if not condition_id:
            return None
        try:
            market = self.public_client.get_clob_market(condition_id)
        except Exception as exc:
            if not record.get("clob_lookup_failed"):
                record["clob_lookup_failed"] = True
                self._log("ERROR", "Gölge kıyas için CLOB sonucu okunamadi", error=str(exc), slug=record.get("slot_slug"))
            return None
        if not market:
            return None
        if not market.slug:
            market.slug = str(record.get("slot_slug") or "")
        if not market.question:
            market.question = str(record.get("question") or "")
        if not market.condition_id:
            market.condition_id = condition_id
        if not market.end_time:
            market.end_time = str(record.get("end_time") or "")
        return market

    def _remember_market_shadow_report(
        self,
        market: MarketSnapshot,
        signal: PredictionSnapshot,
        shadow_result: dict[str, Any],
        entry_allowed: bool,
        entry_reason: str,
    ) -> None:
        record = {
            "reported_at": utc_now_iso(),
            "slot_slug": market.slug,
            "question": market.question,
            "condition_id": market.condition_id,
            "end_time": market.end_time,
            "direction": signal.direction,
            "confidence": round(float(signal.confidence), 4),
            "edge": round(float(signal.expected_edge), 6),
            "entry_allowed": bool(entry_allowed),
            "entry_reason": entry_reason,
            "shadow_result": dict(shadow_result),
        }
        self.pending_shadow_market_reports = [
            item for item in self.pending_shadow_market_reports if str(item.get("slot_slug") or "") != market.slug
        ]
        self.pending_shadow_market_reports.append(record)
        self.pending_shadow_market_reports = self.pending_shadow_market_reports[-60:]

    def _manage_pending_shadow_market_reports(self, current_market: MarketSnapshot | None) -> None:
        if not self.pending_shadow_market_reports:
            return
        for record in list(self.pending_shadow_market_reports):
            market = self._shadow_market_snapshot(record, current_market)
            overdue = self._shadow_market_overdue_seconds(record)
            if overdue > 0 and (
                not market
                or (not market.closed and not market.resolved_outcome)
            ):
                official_market = self._official_resolution_market_for_shadow(record)
                if official_market and (official_market.closed or official_market.resolved_outcome):
                    market = official_market
            if not market:
                continue
            outcome = market.resolved_outcome
            if not outcome and market.closed:
                outcome = self._resolve_outcome_from_market(market, strict=False)
            if not outcome:
                continue
            comparison = dict(record)
            comparison["actual_outcome"] = outcome
            comparison["compared_at"] = utc_now_iso()
            comparison["signal_correct"] = str(record.get("direction") or "").upper() == str(outcome).upper()
            self.shadow_comparison_history.append(comparison)
            self.shadow_comparison_history = self.shadow_comparison_history[-60:]
            self.pending_shadow_market_reports = [
                item for item in self.pending_shadow_market_reports if str(item.get("slot_slug") or "") != str(record.get("slot_slug") or "")
            ]
            self._log(
                "INFO",
                "Gölge kıyas sonucu hazır",
                slug=record.get("slot_slug"),
                actual_outcome=outcome,
                signal_correct=bool(comparison.get("signal_correct")),
                entry_allowed=bool(record.get("entry_allowed")),
                shadow_would_open=bool((record.get("shadow_result") or {}).get("would_open")),
            )
            if self.config.telegram_shadow_market_reports:
                self._notify(self._format_shadow_comparison_report(comparison, outcome))

    def _maybe_send_market_shadow_report(
        self,
        market: MarketSnapshot,
        signal: PredictionSnapshot,
        entry_allowed: bool,
        entry_reason: str,
    ) -> None:
        if not market.slug or self.last_market_shadow_notify_slug == market.slug:
            return
        if int(signal.seconds_into_slot) < int(self.config.min_entry_seconds):
            return
        amount = self._order_size_for_signal(signal)
        shadow_result = self._live_shadow_entry_check(market, signal, amount)
        self.last_market_shadow_notify_slug = market.slug
        self._remember_market_shadow_report(market, signal, shadow_result, entry_allowed, entry_reason)
        if self.config.telegram_shadow_market_reports and self._notify(
            self._format_market_shadow_report(market, signal, shadow_result, entry_allowed, entry_reason)
        ):
            self._log(
                "INFO",
                "Market gölge mod bildirimi gönderildi",
                slug=market.slug,
                would_open=bool(shadow_result.get("would_open")),
                entry_allowed=bool(entry_allowed),
                entry_reason=entry_reason,
                shadow_reason=shadow_result.get("reason"),
            )
            if entry_allowed and not shadow_result.get("would_open"):
                shadow_reason = str(shadow_result.get("reason") or "unknown")
                self.last_paper_shadow_skip_notify_key = f"{market.slug}:{signal.direction}:{shadow_reason}"

    def _live_shadow_notes(self, result: dict[str, Any]) -> list[str]:
        status = "pass" if result.get("would_open") else "fail"
        notes = [
            f"live_shadow={status}",
            f"live_shadow_reason={str(result.get('reason') or 'unknown')}",
            f"live_shadow_order_type={str(result.get('order_type') or 'FOK')}",
        ]
        numeric_keys = {
            "limit_price": "live_shadow_limit",
            "ask": "live_shadow_ask",
            "spread": "live_shadow_spread",
            "book_depth_usdc": "live_shadow_depth_usdc",
            "required_depth_usdc": "live_shadow_required_usdc",
            "book_age_seconds": "live_shadow_book_age_seconds",
        }
        for source, target in numeric_keys.items():
            value = safe_float(result.get(source))
            if value > 0:
                notes.append(f"{target}={round(value, 6)}")
        return notes

    def _live_shadow_stats(self, trades: list[TradeRecord]) -> dict[str, Any]:
        checked = passed = failed = 0
        reasons: dict[str, int] = {}
        for trade in trades:
            status = self._trade_note_value(trade, "live_shadow")
            if status not in {"pass", "fail"}:
                continue
            checked += 1
            if status == "pass":
                passed += 1
            else:
                failed += 1
                reason = self._trade_note_value(trade, "live_shadow_reason") or "unknown"
                reasons[reason] = reasons.get(reason, 0) + 1
        top_reason = ""
        if reasons:
            top_reason = sorted(reasons.items(), key=lambda item: item[1], reverse=True)[0][0]
        return {"checked": checked, "passed": passed, "failed": failed, "top_fail_reason": top_reason}

    def _active_trade_summary_line(self) -> str:
        trade = self.active_trade
        if not trade:
            if self.pending_resolution_trades:
                return f"Bekleyen çözüm: {len(self.pending_resolution_trades)} işlem"
            return "Açık işlem: yok"
        opened_ts = parse_iso_ts(trade.opened_at) or datetime.now(UTC).timestamp()
        age = self._fmt_seconds(datetime.now(UTC).timestamp() - opened_ts)
        line = f"Açık işlem: {trade.side} {trade.mode.upper()} | ${trade.entry_notional_usdc:.2f} @ {trade.entry_price:.3f} | {age}"
        if trade.mode == "paper":
            mark = self._trade_mark_value(trade, self.last_market)
            line += f" | Anlık PnL: {mark - trade.entry_notional_usdc:+.2f}"
        return line

    def _format_telegram_help(self) -> str:
        return (
            "Telegram komutları:\n"
            "/baslat - botu başlatır\n"
            "/durdur - botu durdurur\n"
            "/yeniden_baslat - durdurup tekrar başlatır\n"
            "/rapor - son 5 saatin kısa özeti\n"
            "/bakiye - net bakiye ve başlangıçtan kazanç\n"
            "/durum - anlık bot durumu"
        )

    def _format_balance_report(self, now: datetime | None = None) -> str:
        current_time = now or datetime.now(UTC)
        effective_mode = self._effective_mode(current_time)
        equity = self._paper_equity(self.last_market) if effective_mode == "paper" else self._live_equity()
        totals = self.log_store.total_stats()
        start_ts = parse_iso_ts(self.pnl_tracking_started_at)
        elapsed = self._fmt_seconds(current_time.timestamp() - start_ts) if start_ts else "?"
        if effective_mode == "paper":
            earned = equity - float(self.config.paper_balance_usdc)
        else:
            earned = totals["pnl"]
        lines = [
            f"💰 Bakiye | {current_time.astimezone().strftime('%H:%M')}",
            f"Mod: {effective_mode.upper()}",
            f"Net bakiye: ${equity:.2f}",
            f"Başlangıçtan kazanç: {earned:+.2f} USDC",
            f"Kapalı PnL: {totals['pnl']:+.2f} USDC | İşlem: {totals['count']} ({totals['wins']}K / {totals['losses']}L)",
            f"Başlangıç: {self._fmt_local_datetime(self.pnl_tracking_started_at)} | Süre: {elapsed}",
        ]
        if self.active_trade:
            lines.append(self._active_trade_summary_line())
        return "\n".join(lines)

    def _format_recent_report(self, hours: int = 5, now: datetime | None = None) -> str:
        current_time = now or datetime.now(UTC)
        since_ts = current_time.timestamp() - max(1, int(hours)) * 3600
        closed = sorted(self._closed_trades_since(since_ts), key=lambda trade: parse_iso_ts(trade.closed_at) or 0.0)
        wins = sum(1 for trade in closed if trade.pnl_usdc > 0)
        losses = len(closed) - wins
        net_pnl = round(sum(trade.pnl_usdc for trade in closed), 8)
        total_won = round(sum(trade.pnl_usdc for trade in closed if trade.pnl_usdc > 0), 8)
        total_lost = round(sum(trade.pnl_usdc for trade in closed if trade.pnl_usdc <= 0), 8)
        win_rate = (wins / len(closed) * 100.0) if closed else 0.0
        effective_mode = self._effective_mode(current_time)
        equity = self._paper_equity(self.last_market) if effective_mode == "paper" else self._live_equity()
        shadow_trades = list(closed)
        shadow_trades.extend(
            trade
            for trade in self.pending_resolution_trades
            if (parse_iso_ts(trade.opened_at) or 0.0) >= since_ts
        )
        if self.active_trade and (parse_iso_ts(self.active_trade.opened_at) or 0.0) >= since_ts:
            shadow_trades.append(self.active_trade)
        shadow_stats = self._live_shadow_stats(shadow_trades)
        lines = [
            f"📊 Son {hours} Saat Raporu | {current_time.astimezone().strftime('%H:%M')}",
            f"Mod: {effective_mode.upper()} | Net bakiye: ${equity:.2f}",
            f"İşlem: {len(closed)} | {wins}K / {losses}L | Win: %{win_rate:.1f}",
            f"Net PnL: {net_pnl:+.2f} USDC",
            f"Kazanç: {total_won:+.2f} | Kayıp: {total_lost:+.2f}",
        ]
        if shadow_stats["checked"]:
            shadow_line = f"Live shadow: {shadow_stats['passed']} geçerdi / {shadow_stats['failed']} geçmezdi"
            if shadow_stats["top_fail_reason"]:
                shadow_line += f" | En sık: {shadow_stats['top_fail_reason']}"
            lines.append(shadow_line)
        if closed:
            last = closed[-1]
            result = "K" if last.pnl_usdc > 0 else "L"
            lines.append(f"Son işlem: {last.side} {result} {last.pnl_usdc:+.2f} | {self._fmt_local_time(last.closed_at)}")
        if self.active_trade:
            lines.append(self._active_trade_summary_line())
        return "\n".join(lines)

    def _format_status_report(self, now: datetime | None = None, *, compact: bool = False) -> str:
        current_time = now or datetime.now(UTC)
        running = bool(self.thread and self.thread.is_alive())
        effective_mode = self._effective_mode(current_time)
        equity = self._paper_equity(self.last_market) if effective_mode == "paper" else self._live_equity()
        totals = self.log_store.total_stats()
        lines = [
            f"{'🟢' if running else '🔴'} Durum | {'Çalışıyor' if running else 'Durdu'}",
            f"Mod: {effective_mode.upper()} | Hedef: {self.config.trading_mode.upper()}",
            f"Rollout: {self._rollout_status(current_time)}",
            f"Bakiye: ${equity:.2f} | PnL: {totals['pnl']:+.2f} | Win: %{totals['win_rate_pct']:.1f}",
            f"Cycle: {self.cycle_index} | Son döngü: {self._fmt_local_time(self.last_cycle_finished_at)}",
            self._active_trade_summary_line(),
        ]
        if self.last_signal and not compact:
            lines.append(
                f"Sinyal: {self.last_signal.direction} | Güven: %{self.last_signal.confidence:.1f} | Edge: {self.last_signal.expected_edge:.3f}"
            )
        if self.last_market and not compact:
            lines.append(f"Polymarket: {self._polymarket_market_label(self.last_market)}")
            lines.append(f"Yerel slot: {self._slot_window_label(self.last_market.slug, self.last_market.end_time)}")
        if self.last_live_shadow and not compact:
            lines.append(self._format_live_shadow_result(self.last_live_shadow))
        if not compact:
            feeds = self._feed_status(self.last_market)
            binance_age = safe_float(feeds.get("binance", {}).get("last_receive_age_seconds"), 999999.0)
            poly_age = safe_float(feeds.get("polymarket", {}).get("last_receive_age_seconds"), 999999.0)
            lines.append(f"Feed: Binance {binance_age:.1f}sn | Poly {poly_age:.1f}sn")
        if self.last_error:
            lines.append(f"Son hata: {self.last_error}")
        if self.telegram_command_last_error and not compact:
            lines.append(f"Telegram komut hata: {self.telegram_command_last_error}")
        return "\n".join(lines)

    def _paper_entry_fee_usdc(self, amount_usdc: float, price: float, fee_rate: float) -> float:
        if fee_rate <= 0 or price <= 0:
            return 0.0
        return round(amount_usdc * fee_rate * max(0.0, 1.0 - price), 8)

    def _paper_exit_fee_usdc(self, shares: float, price: float, fee_rate: float) -> float:
        if fee_rate <= 0 or shares <= 0 or price <= 0:
            return 0.0
        return round(shares * fee_rate * price * (1.0 - price), 8)

    def _signal_execution_guard_reason(self, signal: PredictionSnapshot, market: MarketSnapshot, *, prefix: str) -> str:
        chosen_side = market.side(signal.direction)
        spread = max(0.0, round(chosen_side.ask - chosen_side.bid, 6))
        order_amount = self._order_size_for_signal(signal)
        fee_adj_pnl = self._fee_adjusted_pnl_for_signal(signal, market, order_amount)
        signal.features["order_size_usdc"] = round(order_amount, 8)
        signal.features["fee_adj_pnl_effective"] = fee_adj_pnl
        if safe_float(market.liquidity) < float(self.config.live_min_liquidity_usdc):
            self._update_live_confirmation(signal, False)
            return f"{prefix}_liquidity_too_low"
        if spread > float(self.config.live_max_spread):
            self._update_live_confirmation(signal, False)
            return f"{prefix}_spread_too_wide"
        if fee_adj_pnl < float(self.config.live_min_fee_adj_pnl_usdc):
            self._update_live_confirmation(signal, False)
            return f"{prefix}_fee_adj_pnl_too_low"
        confirmations = self._update_live_confirmation(signal, True)
        required_confirmations = max(1, int(self.config.live_signal_confirmations))
        if confirmations < required_confirmations:
            return "waiting_live_confirmation"
        return ""

    def _paper_entry_price(self, market: MarketSnapshot, signal: PredictionSnapshot) -> float:
        side = market.side(signal.direction)
        max_entry_price = self._signal_max_entry_price(signal)
        return max(
            market.tick_size,
            min(max_entry_price, round(side.ask + self.config.market_order_buffer, 4)),
        )

    def _paper_exit_price(self, market: MarketSnapshot, direction: str) -> float:
        side = market.side(direction)
        return max(market.tick_size, round(side.bid, 4))

    def _trade_mark_value(self, trade: TradeRecord, market: MarketSnapshot | None) -> float:
        if not market:
            return trade.shares * trade.entry_price
        side = market.side(trade.side)
        if market.resolved_outcome:
            return trade.shares if market.resolved_outcome == trade.side else 0.0
        mark_price = max(0.0, side.bid)
        mark_value = round(trade.shares * mark_price, 8)
        exit_fee_usdc = self._paper_exit_fee_usdc(trade.shares, mark_price, market.fee_rate)
        return round(max(0.0, mark_value - exit_fee_usdc), 8)

    def _paper_equity(self, market: MarketSnapshot | None = None) -> float:
        equity = self.paper_cash_usdc
        if self.active_trade and self.active_trade.mode == "paper" and self.active_trade.status in {"open", "pending_resolution"}:
            mark_market = market if market and market.slug == self.active_trade.slot_slug else None
            if mark_market is None and self.last_market and self.last_market.slug == self.active_trade.slot_slug:
                mark_market = self.last_market
            equity += self._trade_mark_value(self.active_trade, mark_market)
        for trade in self.pending_resolution_trades:
            if trade.mode != "paper" or trade.status not in {"open", "pending_resolution"}:
                continue
            mark_market = market if market and market.slug == trade.slot_slug else None
            if mark_market is None and self.last_market and self.last_market.slug == trade.slot_slug:
                mark_market = self.last_market
            equity += self._trade_mark_value(trade, mark_market)
        return round(equity, 8)

    def _live_collateral_balance(self) -> float:
        return safe_float(self.last_collateral_status.get("balance"))

    def _live_equity(self) -> float:
        collateral = self._live_collateral_balance()
        open_value = sum(safe_float(item.get("currentValue")) for item in self.last_live_positions)
        return round(collateral + open_value, 8)

    def _base_order_size_usdc(self) -> float:
        return max(5.0, float(self.config.order_size_usdc))

    def _high_confidence_order_size_usdc(self) -> float:
        return max(self._base_order_size_usdc(), float(self.config.high_confidence_order_size_usdc))

    def _max_order_size_usdc(self) -> float:
        return max(self._base_order_size_usdc(), self._high_confidence_order_size_usdc())

    def _order_size_for_signal(self, signal: PredictionSnapshot) -> float:
        threshold = float(self.config.high_confidence_threshold)
        if float(signal.confidence) > threshold:
            return self._high_confidence_order_size_usdc()
        return self._base_order_size_usdc()

    @staticmethod
    def _fair_price_for_signal(signal: PredictionSnapshot) -> float:
        return float(signal.fair_down_price if signal.direction == "DOWN" else signal.fair_up_price)

    def _fee_adjusted_pnl_for_signal(
        self,
        signal: PredictionSnapshot,
        market: MarketSnapshot,
        amount: float,
    ) -> float:
        side = market.side(signal.direction)
        ask = float(side.ask)
        fee_rate = max(0.0, float(market.fee_rate))
        fair_price = self._fair_price_for_signal(signal)
        if ask <= 0 or amount <= 0:
            return -1.0
        net_shares = amount * (1.0 - fee_rate * max(0.0, 1.0 - ask)) / ask
        exit_notional = net_shares * fair_price
        exit_fee = net_shares * fee_rate * fair_price * max(0.0, 1.0 - fair_price)
        return round(exit_notional - exit_fee - amount, 6)

    def _win_rate_pct(self) -> float:
        return self.log_store.total_stats()["win_rate_pct"]

    def _realized_pnl(self) -> float:
        return self.log_store.total_stats()["pnl"]

    def _hourly_stats(self, now: datetime | None = None) -> dict[str, Any]:
        hour_ago = datetime.fromtimestamp(
            (now or datetime.now(UTC)).timestamp() - 3600, tz=UTC
        ).isoformat()
        return self.log_store.hourly_stats(hour_ago)

    @staticmethod
    def _current_local_hour_key(now: datetime | None = None) -> str:
        local_now = (now or datetime.now(UTC)).astimezone()
        return local_now.strftime("%Y-%m-%dT%H")

    @staticmethod
    def _next_local_hour_at(now: datetime | None = None) -> str:
        local_now = (now or datetime.now(UTC)).astimezone()
        next_hour = local_now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return next_hour.isoformat(timespec="seconds")

    @staticmethod
    def _slot_start_ts_from_slug(slug: str) -> float | None:
        try:
            return float(int(str(slug or "").rsplit("-", 1)[-1]))
        except Exception:
            return None

    def _trade_entry_seconds(self, trade: TradeRecord) -> int | None:
        slot_start_ts = self._slot_start_ts_from_slug(trade.slot_slug)
        opened_ts = parse_iso_ts(trade.opened_at)
        if slot_start_ts is None or opened_ts is None:
            return None
        seconds = int(opened_ts - slot_start_ts)
        return max(0, min(299, seconds))

    def _trade_overdue_seconds(self, trade: TradeRecord) -> int:
        end_ts = parse_iso_ts(trade.end_time)
        if end_ts is None:
            return 0
        return int(datetime.now(UTC).timestamp() - end_ts)

    def _detect_live_sdk_package(self) -> str:
        for package_name in ("py-clob-client-v2", "py-clob-client"):
            try:
                version = importlib_metadata.version(package_name)
                return f"{package_name}@{version}"
            except importlib_metadata.PackageNotFoundError:
                continue
            except Exception:
                continue
        return "unknown"

    def _reset_live_confirmation(self) -> None:
        self.live_confirmation_slot = ""
        self.live_confirmation_direction = ""
        self.live_confirmation_count = 0

    def _clear_live_suspension(self) -> None:
        self.live_suspended_reason = ""
        self.live_suspended_at = ""
        self.live_order_failures = 0
        self._reset_live_confirmation()

    def _set_live_suspension(self, reason: str, *, notify: bool = True) -> None:
        clean_reason = str(reason or "").strip().lower()
        if not clean_reason:
            return
        if clean_reason == self.live_suspended_reason:
            return
        self.live_suspended_reason = clean_reason
        self.live_suspended_at = utc_now_iso()
        self._reset_live_confirmation()
        self._log("ERROR", "Live koruma kilidi aktif", reason=clean_reason)
        if notify:
            self._notify(
                "LIVE KORUMA KILIDI\n"
                f"Sebep: {clean_reason}\n"
                "Yeni live emirler askiya alindi. Bot paper fallback ile devam eder.",
                respect_trade_toggle=False,
            )

    def _trade_age_seconds(self, trade: TradeRecord) -> int:
        opened_ts = parse_iso_ts(trade.opened_at)
        if opened_ts is None:
            return 0
        return max(0, int(datetime.now(UTC).timestamp() - opened_ts))

    def _live_closed_trades(self) -> list[TradeRecord]:
        return [
            trade
            for trade in self.closed_trades
            if trade.mode == "live" and trade.status in {"closed", "resolved", "redeemable"}
        ]

    def _live_daily_pnl(self, now: datetime | None = None) -> float:
        current_local_date = (now or datetime.now(UTC)).astimezone().date()
        total = 0.0
        for trade in self._live_closed_trades():
            if not trade.closed_at:
                continue
            try:
                closed_local_date = datetime.fromisoformat(trade.closed_at.replace("Z", "+00:00")).astimezone().date()
            except Exception:
                continue
            if closed_local_date != current_local_date:
                continue
            total += trade.pnl_usdc
        return round(total, 8)

    def _live_consecutive_losses(self) -> int:
        ordered = sorted(
            self._live_closed_trades(),
            key=lambda trade: parse_iso_ts(trade.closed_at) or 0.0,
        )
        streak = 0
        for trade in reversed(ordered):
            if trade.pnl_usdc < 0:
                streak += 1
                continue
            break
        return streak

    def _clob_v2_guard_reason(self, now: datetime | None = None) -> str:
        if not self.config.live_clob_v2_guard:
            return ""
        current_time = now or datetime.now(UTC)
        guard_after = datetime(2026, 4, 28, 10, 30, tzinfo=UTC)
        if current_time < guard_after:
            return ""
        if self.live_sdk_package.startswith("py-clob-client-v2@"):
            return ""
        return "clob_v2_upgrade_required"

    def _update_live_confirmation(self, signal: PredictionSnapshot, passed: bool) -> int:
        slot = str(signal.slug or "")
        direction = signal.direction if signal.direction in {"UP", "DOWN"} else ""
        if not slot or not direction:
            self.live_confirmation_slot = ""
            self.live_confirmation_direction = ""
            self.live_confirmation_count = 0
            return 0
        if self.live_confirmation_slot != slot or self.live_confirmation_direction != direction:
            self.live_confirmation_slot = slot
            self.live_confirmation_direction = direction
            self.live_confirmation_count = 1 if passed else 0
            return self.live_confirmation_count
        self.live_confirmation_count = self.live_confirmation_count + 1 if passed else 0
        return self.live_confirmation_count

    def _live_guard_reason(self, signal: PredictionSnapshot, market: MarketSnapshot, now: datetime | None = None) -> str:
        if not self.config.live_hard_mode:
            return ""
        current_time = now or datetime.now(UTC)
        if self.live_suspended_reason:
            return f"live_suspended:{self.live_suspended_reason}"

        migration_reason = self._clob_v2_guard_reason(current_time)
        if migration_reason:
            self._set_live_suspension(migration_reason)
            return f"live_suspended:{migration_reason}"

        daily_loss_limit = abs(float(self.config.live_max_daily_loss_usdc))
        if daily_loss_limit > 0 and self._live_daily_pnl(current_time) <= -daily_loss_limit:
            self._set_live_suspension("daily_loss_limit_hit")
            return "live_suspended:daily_loss_limit_hit"

        max_loss_streak = max(1, int(self.config.live_max_consecutive_losses))
        if self._live_consecutive_losses() >= max_loss_streak:
            self._set_live_suspension("consecutive_losses_limit_hit")
            return "live_suspended:consecutive_losses_limit_hit"

        max_failures = max(1, int(self.config.live_max_order_failures))
        if self.live_order_failures >= max_failures:
            self._set_live_suspension("order_failures_limit_hit")
            return "live_suspended:order_failures_limit_hit"

        return self._signal_execution_guard_reason(signal, market, prefix="live")

    def _mark_live_order_failure(self, error: Exception | str) -> None:
        self.live_order_failures += 1
        self._log(
            "ERROR",
            "Live emir acilamadi",
            error=str(error),
            failure_count=int(self.live_order_failures),
        )
        self._notify(
            "LIVE EMIR HATASI\n"
            f"Hata: {str(error)}\n"
            f"Ardisik hata sayisi: {int(self.live_order_failures)}",
            respect_trade_toggle=False,
        )
        if self.live_order_failures >= max(1, int(self.config.live_max_order_failures)):
            self._set_live_suspension("order_failures_limit_hit")

    def _loop_wait_seconds(self) -> float:
        base_wait = max(1, int(self.config.poll_seconds))
        fast_wait = max(1, int(self.config.resolution_poll_seconds))
        fast_window = max(fast_wait, int(self.config.fast_poll_window_seconds))
        entry_wait = float(min(base_wait, max(2, fast_wait * 2)))

        trade = self.active_trade
        if not trade and self.pending_resolution_trades:
            return float(fast_wait)
        if not trade:
            signal = self.last_signal
            if signal:
                seconds_into_slot = max(0, int(signal.seconds_into_slot))
                seconds_to_close = max(0, int(signal.seconds_to_close))
                entry_start = max(0, int(self.config.min_entry_seconds))
                entry_end = max(entry_start, int(self.config.max_entry_seconds))
                entry_lead = min(30, max(10, base_wait * 2))
                if seconds_to_close <= max(int(self.config.min_seconds_left_to_trade) + 5, fast_window):
                    return float(fast_wait)
                if max(0, entry_start - entry_lead) <= seconds_into_slot <= entry_end:
                    return entry_wait
            return float(base_wait)
        if trade.status in {"pending_resolution", "redeemable"}:
            return float(fast_wait)

        end_ts = parse_iso_ts(trade.end_time)
        if end_ts is None:
            return float(base_wait)
        seconds_to_close = end_ts - datetime.now(UTC).timestamp()
        if seconds_to_close <= 0:
            return float(fast_wait)
        if seconds_to_close <= fast_window:
            return float(min(base_wait, fast_wait))
        return float(base_wait)

    def _queue_pending_resolution(self, trade: TradeRecord, *, notify: bool = True) -> None:
        trade.status = "pending_resolution"
        exists = next((item for item in self.pending_resolution_trades if item.trade_id == trade.trade_id), None)
        if exists is None:
            self.pending_resolution_trades.append(trade)
        if self.active_trade and self.active_trade.trade_id == trade.trade_id:
            self.active_trade = None
        if notify and "system:queued_for_resolution" not in trade.notes:
            trade.notes.append("system:queued_for_resolution")
            slot_str = self._slot_window_label(trade.slot_slug, trade.end_time)
            cycle_str = self._entry_cycle_label(trade)
            self._log("INFO", "Trade resolve kuyruğuna alındı", slug=trade.slot_slug, mode=trade.mode)
            self._notify(
                "⏳ RESOLVE KUYRUGU — Resmi sonuc bekleniyor\n"
                f"Saat Araligi: {slot_str} | Cycle: {cycle_str}\n"
                f"Yön: {trade.side} | Alış: {trade.entry_price:.3f}"
            )

    def _remove_pending_resolution_trade(self, trade_id: str) -> None:
        self.pending_resolution_trades = [
            item for item in self.pending_resolution_trades if item.trade_id != trade_id
        ]

    def _trade_market_snapshot(self, trade: TradeRecord, current_market: MarketSnapshot | None) -> MarketSnapshot | None:
        if current_market and current_market.slug == trade.slot_slug:
            return current_market
        return self.public_client.get_market_by_slug(trade.slot_slug)

    def _record_closed_trade(self, trade: TradeRecord) -> None:
        self.closed_trades = [item for item in self.closed_trades if item.trade_id != trade.trade_id]
        self.closed_trades.append(trade)
        self.closed_trades = self.closed_trades[-60:]
        self.log_store.upsert_trade(trade)

    def _official_resolution_market(self, trade: TradeRecord) -> MarketSnapshot | None:
        try:
            market = self.public_client.get_clob_market(trade.condition_id)
        except Exception as exc:
            if "system:clob_lookup_failed" not in trade.notes:
                trade.notes.append("system:clob_lookup_failed")
                self._log("ERROR", "CLOB resmi market sonucu okunamadi", error=str(exc), slug=trade.slot_slug)
            return None
        if not market:
            return None
        if not market.slug:
            market.slug = trade.slot_slug
        if not market.question:
            market.question = trade.question
        if not market.condition_id:
            market.condition_id = trade.condition_id
        if not market.end_time:
            market.end_time = trade.end_time
        if market.resolved_outcome and "system:clob_resolution_source" not in trade.notes:
            trade.notes.append("system:clob_resolution_source")
            self._log(
                "INFO",
                "Resmi sonuc CLOB market endpointinden alindi",
                slug=trade.slot_slug,
                outcome=market.resolved_outcome,
            )
        return market

    def _cycle_number_for_seconds(self, seconds_into_slot: int) -> int:
        poll = max(1, int(self.config.poll_seconds))
        bounded_seconds = max(0, min(299, int(seconds_into_slot)))
        return int(bounded_seconds // poll) + 1

    def _entry_cycle_label(self, trade: TradeRecord) -> str:
        entry_seconds = self._trade_entry_seconds(trade)
        if entry_seconds is None:
            return "?"
        return f"C{self._cycle_number_for_seconds(entry_seconds)}"

    def _slot_window_label(self, slug: str, fallback_end: str = "") -> str:
        slot_start_ts = self._slot_start_ts_from_slug(slug)
        if slot_start_ts is not None:
            start_dt = datetime.fromtimestamp(slot_start_ts, UTC).astimezone()
            end_dt = (datetime.fromtimestamp(slot_start_ts, UTC) + timedelta(minutes=5)).astimezone()
            return f"{start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}"
        try:
            end_dt = datetime.fromisoformat(fallback_end.replace("Z", "+00:00")).astimezone()
            start_dt = end_dt - timedelta(minutes=5)
            return f"{start_dt.strftime('%H:%M')} → {end_dt.strftime('%H:%M')}"
        except Exception:
            return "?"

    def _cycle_analytics(self) -> dict[str, Any]:
        closed = [trade for trade in self.closed_trades if trade.status in {"closed", "resolved"}]
        if not closed:
            return {"best_cycle": None, "cycle_buckets": [], "sample_count": 0}

        bucket_size = 30
        poll = max(1, int(self.config.poll_seconds))
        grouped: dict[int, dict[str, Any]] = {}
        for trade in closed:
            entry_seconds = self._trade_entry_seconds(trade)
            if entry_seconds is None:
                continue
            bucket_start = (entry_seconds // bucket_size) * bucket_size
            row = grouped.setdefault(
                bucket_start,
                {
                    "bucket_start_seconds": bucket_start,
                    "bucket_end_seconds": min(299, bucket_start + bucket_size - 1),
                    "sample_count": 0,
                    "wins": 0,
                    "total_pnl_usdc": 0.0,
                    "entry_seconds_total": 0,
                },
            )
            row["sample_count"] += 1
            row["wins"] += 1 if trade.pnl_usdc > 0 else 0
            row["total_pnl_usdc"] += trade.pnl_usdc
            row["entry_seconds_total"] += entry_seconds

        buckets: list[dict[str, Any]] = []
        for bucket_start in sorted(grouped):
            row = grouped[bucket_start]
            sample_count = int(row["sample_count"])
            avg_pnl = row["total_pnl_usdc"] / sample_count if sample_count else 0.0
            avg_entry_seconds = row["entry_seconds_total"] / sample_count if sample_count else 0.0
            win_rate_pct = row["wins"] / sample_count * 100.0 if sample_count else 0.0
            cycle_start = int(row["bucket_start_seconds"] // poll) + 1
            cycle_end = int(row["bucket_end_seconds"] // poll) + 1
            ranking_score = avg_pnl * min(1.0, sample_count / 2.0)
            buckets.append(
                {
                    "label": f"{int(row['bucket_start_seconds']):03d}-{int(row['bucket_end_seconds']):03d} sn",
                    "cycle_range": f"C{cycle_start}-C{cycle_end}",
                    "bucket_start_seconds": int(row["bucket_start_seconds"]),
                    "bucket_end_seconds": int(row["bucket_end_seconds"]),
                    "avg_entry_seconds": round(avg_entry_seconds, 2),
                    "sample_count": sample_count,
                    "wins": int(row["wins"]),
                    "win_rate_pct": round(win_rate_pct, 2),
                    "total_pnl_usdc": round(row["total_pnl_usdc"], 6),
                    "avg_pnl_usdc": round(avg_pnl, 6),
                    "ranking_score": round(ranking_score, 6),
                }
            )

        ranked = sorted(
            buckets,
            key=lambda item: (item["ranking_score"], item["sample_count"], item["avg_pnl_usdc"]),
            reverse=True,
        )
        return {
            "best_cycle": ranked[0] if ranked else None,
            "cycle_buckets": ranked[:8],
            "sample_count": len(closed),
        }

    def _format_hourly_report(self, now: datetime | None = None) -> str:
        current_time = now or datetime.now(UTC)
        local_now = current_time.astimezone()
        effective_mode = self._effective_mode(current_time)
        equity = self._paper_equity(self.last_market) if effective_mode == "paper" else self._live_equity()

        h = self._hourly_stats(current_time)
        totals = self.log_store.total_stats()

        lines = [
            f"📊 Saatlik Rapor | {local_now.strftime('%H:%M')}",
            f"Mod: {effective_mode.upper()} | Bakiye: ${equity:.2f}",
            "─────────────────",
            f"Bu saat PnL: {h['net_pnl']:+.2f} USDC | İşlem: {h['count']} ({h['win_count']}K / {h['loss_count']}L)",
            f"  Kazanç: {h['total_won']:+.2f} USDC | Kayıp: {h['total_lost']:+.2f} USDC",
            "─────────────────",
            f"Toplam PnL (reset sonrası): {totals['pnl']:+.2f} USDC",
            f"Toplam işlem: {totals['count']} | {totals['wins']}K / {totals['losses']}L | Win: %{totals['win_rate_pct']:.1f}",
        ]
        if self.pnl_tracking_started_at:
            lines.append(f"PnL başlangıç: {self._fmt_local_time(self.pnl_tracking_started_at)}")
        if self.live_suspended_reason:
            lines.append(f"Live guard: {self.live_suspended_reason}")

        return "\n".join(lines)

    def _maybe_send_hourly_report(self, now: datetime | None = None) -> None:
        if not self.config.telegram_hourly_reports or not self.notifier.configured:
            return
        current_time = now or datetime.now(UTC)
        hour_key = self._current_local_hour_key(current_time)
        if not hour_key or hour_key == self.last_hourly_report_key:
            return
        self.last_hourly_report_key = hour_key
        if self._notify(self._format_hourly_report(current_time), respect_trade_toggle=False):
            self.last_hourly_report_sent_at = utc_now_iso()
            self._log("INFO", "Saatlik Telegram raporu gonderildi", hour_key=hour_key)

    def _entry_permitted(self, signal: PredictionSnapshot, market: MarketSnapshot) -> tuple[bool, str]:
        effective_mode = self._effective_mode()
        order_amount = self._order_size_for_signal(signal)
        if self.active_trade:
            if self.active_trade.status == "pending_resolution":
                return False, "waiting_resolution"
            if self.active_trade.status in {"open", "redeemable"}:
                return False, "open_trade_exists"
            return False, "active_trade_exists"
        if self.last_traded_slot_slug == market.slug:
            return False, "slot_already_traded"
        if not signal.entry_allowed:
            return False, signal.reason
        min_entry = float(getattr(self.config, "min_entry_price", 0.0))
        if min_entry > 0:
            ask = market.side(signal.direction).ask
            if ask < min_entry:
                return False, f"entry_price_too_low:{ask:.3f}<{min_entry:.3f}"
        if effective_mode == "live":
            if not self.live_client or not self.live_client.configured:
                return False, "polymarket_live_secret_missing"
            if self.last_geoblock.get("blocked"):
                return False, "geoblocked"
            if self._live_collateral_balance() < order_amount:
                return False, "insufficient_collateral"
            data_guard_reason = self._live_data_guard_reason(signal, market)
            if data_guard_reason:
                return False, data_guard_reason
            live_guard_reason = self._live_guard_reason(signal, market)
            if live_guard_reason:
                return False, live_guard_reason
        else:
            if self.paper_cash_usdc < order_amount:
                return False, "insufficient_paper_cash"
        return True, "ok"

    def _open_paper_trade(self, market: MarketSnapshot, signal: PredictionSnapshot) -> None:
        side = market.side(signal.direction)
        amount = self._order_size_for_signal(signal)
        sizing_tier = "high_confidence" if amount > self._base_order_size_usdc() else "base"
        entry_price = self._paper_entry_price(market, signal)
        shadow_result = self._live_shadow_entry_check(market, signal, amount)
        if self.config.paper_require_live_shadow and not shadow_result.get("would_open"):
            shadow_reason = str(shadow_result.get("reason") or "unknown")
            self._log(
                "INFO",
                "Paper pozisyon live shadow nedeniyle atlandı",
                reason=shadow_reason,
                slug=market.slug,
                direction=signal.direction,
                ask=entry_price,
            )
            notify_key = f"{market.slug}:{signal.direction}:{shadow_reason}"
            if notify_key != self.last_paper_shadow_skip_notify_key:
                self.last_paper_shadow_skip_notify_key = notify_key
                self._notify(
                    f"🟡 PAPER PAS — Gölge mod geçmedi\n"
                    f"Yön: {signal.direction} | Paper ask: {entry_price:.3f} | Tutar: ${amount:.2f}\n"
                    f"{self._format_live_shadow_result(shadow_result)}\n"
                    f"Güven: %{signal.confidence:.1f} | Edge: {signal.expected_edge:.3f}"
                )
            raise RuntimeError(f"paper_live_shadow_failed:{shadow_reason}")
        shadow_limit_price = safe_float(shadow_result.get("limit_price"))
        if shadow_limit_price > 0:
            entry_price = max(market.tick_size, round(shadow_limit_price, 4))
        else:
            entry_price = self._paper_entry_price(market, signal)
        gross_shares = amount / entry_price
        entry_fee_usdc = self._paper_entry_fee_usdc(amount, entry_price, market.fee_rate)
        fee_shares = entry_fee_usdc / entry_price if entry_price > 0 else 0.0
        net_shares = max(0.0, gross_shares - fee_shares)
        self.paper_cash_usdc = round(self.paper_cash_usdc - amount, 8)
        self.active_trade = TradeRecord(
            trade_id=uuid.uuid4().hex,
            mode="paper",
            slot_slug=market.slug,
            question=market.question,
            condition_id=market.condition_id,
            side=signal.direction,
            token_id=side.token_id,
            end_time=market.end_time,
            shares=round(net_shares, 8),
            entry_price=round(entry_price, 4),
            entry_notional_usdc=round(amount, 8),
            entry_fee_usdc=round(entry_fee_usdc, 8),
            opened_at=utc_now_iso(),
            notes=[
                f"confidence={signal.confidence}",
                f"edge={signal.expected_edge}",
                f"order_size_usdc={amount}",
                f"sizing_tier={sizing_tier}",
            ] + self._live_shadow_notes(shadow_result),
        )
        self._log("INFO", "Paper pozisyon açıldı", side=signal.direction, slug=market.slug, ask=entry_price, shares=round(net_shares, 6))
        _cycle_str = self._entry_cycle_label(self.active_trade)
        _dir_emoji = "📈" if signal.direction == "UP" else "📉"
        _now_str = datetime.now(UTC).astimezone().strftime("%H:%M:%S")
        _slot_str = self._slot_window_label(market.slug, market.end_time)
        _poly_market_str = self._polymarket_market_label(market)
        self._notify(
            f"{_dir_emoji} AÇILIŞ — {signal.direction} | Paper\n"
            f"Bot saati: {_now_str} | Yerel slot: {_slot_str}\n"
            f"Polymarket: {_poly_market_str}\n"
            f"Cycle: {_cycle_str}\n"
            f"Alış: {entry_price:.3f} | Tutar: ${amount:.2f}\n"
            f"Pay: {net_shares:.4f} | Komisyon: ${entry_fee_usdc:.4f}\n"
            f"{self._format_live_shadow_result(shadow_result)}\n"
            f"Güven: %{signal.confidence:.1f} | Edge: {signal.expected_edge:.3f}"
        )

    def _open_live_trade(self, market: MarketSnapshot, signal: PredictionSnapshot) -> None:
        assert self.live_client is not None
        amount = self._order_size_for_signal(signal)
        market = self._fresh_live_market_for_order(market, signal, amount)
        side = market.side(signal.direction)
        sizing_tier = "high_confidence" if amount > self._base_order_size_usdc() else "base"
        limit_price = self._live_entry_limit_price(side.ask, signal)
        response = self.live_client.buy(side, amount, limit_price)
        if not response.get("success"):
            raise RuntimeError(str(response.get("errorMsg") or "live order rejected"))
        est_shares = amount / max(limit_price, market.tick_size)
        self.active_trade = TradeRecord(
            trade_id=uuid.uuid4().hex,
            mode="live",
            slot_slug=market.slug,
            question=market.question,
            condition_id=market.condition_id,
            side=signal.direction,
            token_id=side.token_id,
            end_time=market.end_time,
            shares=round(est_shares, 8),
            entry_price=round(limit_price, 4),
            entry_notional_usdc=round(amount, 8),
            opened_at=utc_now_iso(),
            entry_order_id=str(response.get("orderID", "")),
            live_order_status=str(response.get("status", "")),
            notes=[
                "local size is an estimate until synced from positions",
                f"confidence={signal.confidence}",
                f"edge={signal.expected_edge}",
                f"order_size_usdc={amount}",
                f"sizing_tier={sizing_tier}",
            ],
        )
        self.live_order_failures = 0
        self.live_confirmation_slot = ""
        self.live_confirmation_direction = ""
        self.live_confirmation_count = 0
        try:
            self._sync_live_position(self.active_trade, refresh_collateral=False)
            if self.active_trade.entry_order_id:
                order_state = self.live_client.get_order(self.active_trade.entry_order_id)
                if isinstance(order_state, dict):
                    order_status = str(
                        order_state.get("status")
                        or (order_state.get("order") or {}).get("status")
                        or self.active_trade.live_order_status
                    )
                    self.active_trade.live_order_status = order_status
        except Exception as exc:
            self._log("WARNING", "Live emir ilk sync kontrolden gecemedi", error=str(exc), slug=market.slug)
        if self.config.live_require_synced_position and not self.active_trade.synced_size:
            self.active_trade.notes.append("system:position_sync_pending")
            self._log(
                "WARNING",
                "Live pozisyon size sync bekliyor",
                slug=market.slug,
                order_id=self.active_trade.entry_order_id,
            )
        self._log("INFO", "Live pozisyon açıldı", side=signal.direction, slug=market.slug, limit_price=limit_price, order_id=response.get("orderID", ""))
        _cycle_str = self._entry_cycle_label(self.active_trade)
        _dir_emoji = "📈" if signal.direction == "UP" else "📉"
        _now_str = datetime.now(UTC).astimezone().strftime("%H:%M:%S")
        _slot_str = self._slot_window_label(market.slug, market.end_time)
        _poly_market_str = self._polymarket_market_label(market)
        _depth_usdc = safe_float(signal.features.get("live_orderbook_depth_usdc"))
        _required_depth = safe_float(signal.features.get("live_orderbook_required_usdc"))
        _book_age = safe_float(signal.features.get("live_orderbook_age_seconds"))
        self._notify(
            f"{_dir_emoji} AÇILIŞ — {signal.direction} | Live\n"
            f"Bot saati: {_now_str} | Yerel slot: {_slot_str}\n"
            f"Polymarket: {_poly_market_str}\n"
            f"Cycle: {_cycle_str}\n"
            f"Alış: {limit_price:.3f} | Tutar: ${amount:.2f}\n"
            f"FOK: evet | Book: ${_depth_usdc:.2f}/${_required_depth:.2f} | Yaş: {_book_age:.2f}sn\n"
            f"Order: {response.get('orderID', '-')}\n"
            f"Güven: %{signal.confidence:.1f} | Edge: {signal.expected_edge:.3f}"
        )

    def _open_trade(self, market: MarketSnapshot, signal: PredictionSnapshot) -> None:
        if self._effective_mode() == "paper":
            self._open_paper_trade(market, signal)
        else:
            self._open_live_trade(market, signal)
        self.last_traded_slot_slug = market.slug

    def _sync_live_position(self, trade: TradeRecord, refresh_collateral: bool = True) -> dict[str, Any]:
        if not self.live_client:
            return {}
        if refresh_collateral:
            try:
                self.last_collateral_status = self.live_client.collateral_status()
            except Exception as exc:
                self._log("ERROR", "Collateral durumu okunamadı", error=str(exc))
        try:
            positions = self.public_client.get_positions(self.live_client.profile_address, trade.condition_id)
        except Exception as exc:
            self._log("ERROR", "Live positions okunamadı", error=str(exc))
            return {}
        self.last_live_positions = positions
        outcome_label = "Up" if trade.side == "UP" else "Down"
        for item in positions:
            if str(item.get("outcome", "")).strip().lower() != outcome_label.lower():
                continue
            trade.shares = round(safe_float(item.get("size"), trade.shares), 8)
            avg_price = safe_float(item.get("avgPrice"), trade.entry_price)
            if avg_price > 0:
                trade.entry_price = round(avg_price, 4)
            trade.synced_size = True
            trade.redeemable = bool(item.get("redeemable", False))
            return item
        return {}

    def _close_paper_trade(self, market: MarketSnapshot, exit_reason: str) -> None:
        assert self.active_trade is not None
        trade = self.active_trade
        exit_price = self._paper_exit_price(market, trade.side)
        exit_notional = round(trade.shares * exit_price, 8)
        exit_fee_usdc = self._paper_exit_fee_usdc(trade.shares, exit_price, market.fee_rate)
        net_proceeds = round(exit_notional - exit_fee_usdc, 8)
        self.paper_cash_usdc = round(self.paper_cash_usdc + net_proceeds, 8)
        pnl = round(net_proceeds - trade.entry_notional_usdc, 8)
        trade.status = "closed"
        trade.exit_price = round(exit_price, 4)
        trade.exit_notional_usdc = exit_notional
        trade.exit_fee_usdc = exit_fee_usdc
        trade.pnl_usdc = pnl
        trade.pnl_pct = round((pnl / trade.entry_notional_usdc * 100.0) if trade.entry_notional_usdc > 0 else 0.0, 6)
        trade.closed_at = utc_now_iso()
        trade.exit_reason = exit_reason
        self._record_closed_trade(trade)
        self.active_trade = None
        self._log("INFO", "Paper pozisyon kapandı", reason=exit_reason, pnl_usdc=pnl, slug=market.slug)
        _total_fee = round(trade.entry_fee_usdc + exit_fee_usdc, 4)
        _pnl_emoji = "✅" if pnl > 0 else "❌"
        _dir_emoji = "📈" if trade.side == "UP" else "📉"
        self._notify(
            f"{_dir_emoji} KAPANIŞ — {exit_reason} | Paper\n"
            f"Yön: {trade.side} | {trade.entry_price:.3f} → {exit_price:.3f}\n"
            f"Tutar: ${trade.entry_notional_usdc:.2f} → ${exit_notional:.2f}\n"
            f"Komisyon+Fee: ${_total_fee:.4f}\n"
            f"Net PnL: {pnl:+.4f} USDC {_pnl_emoji}"
        )

    def _close_live_trade(self, market: MarketSnapshot, exit_reason: str) -> None:
        assert self.active_trade is not None and self.live_client is not None
        trade = self.active_trade
        synced = self._sync_live_position(trade, refresh_collateral=False)
        shares = trade.shares if trade.shares > 0 else safe_float(synced.get("size"))
        if shares <= 0:
            raise RuntimeError("live position size unavailable")
        side = market.side(trade.side)
        limit_price = max(market.tick_size, round(side.bid - self.config.market_order_buffer, 4))
        response = self.live_client.sell(side, shares, limit_price)
        if not response.get("success"):
            raise RuntimeError(str(response.get("errorMsg") or "live exit rejected"))
        exit_price = max(limit_price, market.tick_size)
        exit_notional = round(shares * exit_price, 8)
        exit_fee_usdc = self._paper_exit_fee_usdc(shares, exit_price, market.fee_rate)
        pnl = round(exit_notional - exit_fee_usdc - trade.entry_notional_usdc, 8)
        trade.status = "closed"
        trade.exit_price = round(exit_price, 4)
        trade.exit_notional_usdc = exit_notional
        trade.exit_fee_usdc = exit_fee_usdc
        trade.pnl_usdc = pnl
        trade.pnl_pct = round((pnl / trade.entry_notional_usdc * 100.0) if trade.entry_notional_usdc > 0 else 0.0, 6)
        trade.closed_at = utc_now_iso()
        trade.exit_reason = exit_reason
        trade.exit_order_id = str(response.get("orderID", ""))
        trade.live_order_status = str(response.get("status", ""))
        trade.notes.append("live pnl locally estimated from limit price and fee model")
        self._record_closed_trade(trade)
        self.active_trade = None
        self.live_confirmation_slot = ""
        self.live_confirmation_direction = ""
        self.live_confirmation_count = 0
        self._log("INFO", "Live pozisyon kapandı", reason=exit_reason, pnl_estimate=pnl, slug=market.slug)
        _total_fee = round(trade.entry_fee_usdc + exit_fee_usdc, 4)
        _pnl_emoji = "✅" if pnl > 0 else "❌"
        _dir_emoji = "📈" if trade.side == "UP" else "📉"
        self._notify(
            f"{_dir_emoji} KAPANIŞ — {exit_reason} | Live\n"
            f"Yön: {trade.side} | {trade.entry_price:.3f} → {exit_price:.3f}\n"
            f"Tutar: ${trade.entry_notional_usdc:.2f} → ${exit_notional:.2f}\n"
            f"Komisyon+Fee: ${_total_fee:.4f}\n"
            f"Net PnL: {pnl:+.4f} USDC {_pnl_emoji} (tahmini)"
        )

    def _close_trade(self, market: MarketSnapshot, exit_reason: str) -> None:
        if not self.active_trade:
            return
        if self.active_trade.mode == "paper":
            self._close_paper_trade(market, exit_reason)
        else:
            self._close_live_trade(market, exit_reason)

    @staticmethod
    def _resolve_outcome_from_market(market: MarketSnapshot, strict: bool = False) -> str:
        """Resmi sonuç varsa onu döndür; yoksa kapanış fiyatından türet.

        strict=True → sadece kesine yakın fark (>=0.95) varsa karar ver.
        Canlı trade'lerde kullanılır — bulanık fiyatta yanlış settle olmasın."""
        if market.resolved_outcome:
            return market.resolved_outcome
        if market.closed:
            up_p = market.up.price
            down_p = market.down.price
            if strict:
                if up_p >= 0.95 and down_p <= 0.05:
                    return "UP"
                if down_p >= 0.95 and up_p <= 0.05:
                    return "DOWN"
                return ""
            return "UP" if up_p >= down_p else "DOWN"
        return ""

    def _settle_trade(self, trade: TradeRecord, market: MarketSnapshot) -> None:
        if not market.resolved_outcome:
            return
        payout = round(trade.shares if trade.side == market.resolved_outcome else 0.0, 8)
        pnl = round(payout - trade.entry_notional_usdc, 8)
        slot_str = self._slot_window_label(trade.slot_slug, trade.end_time)
        cycle_str = self._entry_cycle_label(trade)
        trade.exit_price = 1.0 if payout > 0 else 0.0
        trade.exit_notional_usdc = payout
        trade.pnl_usdc = pnl
        trade.pnl_pct = round((pnl / trade.entry_notional_usdc * 100.0) if trade.entry_notional_usdc > 0 else 0.0, 6)
        trade.closed_at = utc_now_iso()
        trade.resolution_outcome = market.resolved_outcome
        self._remove_pending_resolution_trade(trade.trade_id)
        if self.active_trade and self.active_trade.trade_id == trade.trade_id:
            self.active_trade = None
        if trade.mode == "paper":
            self.paper_cash_usdc = round(self.paper_cash_usdc + payout, 8)
            trade.status = "resolved"
            trade.exit_reason = "resolved"
            self._record_closed_trade(trade)
            self._log("INFO", "Paper pozisyon resolve oldu", outcome=market.resolved_outcome, pnl_usdc=pnl, slug=market.slug)
            _won = market.resolved_outcome == trade.side
            _pnl_emoji = "✅" if _won else "❌"
            _outcome_emoji = "🟢" if _won else "🔴"
            self._notify(
                f"{_outcome_emoji} RESOLVE — {market.resolved_outcome} | Paper\n"
                f"Saat Araligi: {slot_str} | Cycle: {cycle_str}\n"
                f"Yön: {trade.side} | Alış: {trade.entry_price:.3f}\n"
                f"Resmi Sonuç: {market.resolved_outcome} | Payout: ${payout:.4f}\n"
                f"Komisyon+Fee: ${trade.entry_fee_usdc:.4f}\n"
                f"Net PnL: {pnl:+.4f} USDC {_pnl_emoji}"
            )
            return
        self._sync_live_position(trade, refresh_collateral=False)
        trade.status = "redeemable"
        trade.redeemable = True
        trade.exit_reason = "resolved_waiting_redeem"
        trade.notes.append("winning shares may require manual redeem if not sold before close")
        self._record_closed_trade(trade)
        self.live_confirmation_slot = ""
        self.live_confirmation_direction = ""
        self.live_confirmation_count = 0
        self._log("WARNING", "Live pozisyon resolve oldu, redeem gerekli olabilir", outcome=market.resolved_outcome, slug=market.slug)
        _won = market.resolved_outcome == trade.side
        _outcome_emoji = "🟢" if _won else "🔴"
        self._notify(
            f"{_outcome_emoji} RESOLVE — {market.resolved_outcome} | Live\n"
            f"Saat Araligi: {slot_str} | Cycle: {cycle_str}\n"
            f"Yön: {trade.side} | Alış: {trade.entry_price:.3f}\n"
            f"Resmi Sonuç: {market.resolved_outcome}\n"
            f"Tahmini PnL: {pnl:+.4f} USDC {'✅' if _won else '❌'}\n"
            f"⚠️ Paylar redeem bekliyor olabilir."
        )

    def _manage_pending_resolution_trades(self, current_market: MarketSnapshot | None) -> None:
        if not self.pending_resolution_trades:
            return
        for trade in list(self.pending_resolution_trades):
            if trade.status == "redeemable":
                self._remove_pending_resolution_trade(trade.trade_id)
                continue
            overdue = self._trade_overdue_seconds(trade)
            trade_market = self._trade_market_snapshot(trade, current_market)
            if overdue > 0 and (
                not trade_market
                or (not trade_market.closed and not trade_market.resolved_outcome)
            ):
                official_market = self._official_resolution_market(trade)
                if official_market and (official_market.closed or official_market.resolved_outcome):
                    trade_market = official_market
            if not trade_market:
                if overdue > 0 and "system:market_missing_after_close" not in trade.notes:
                    trade.notes.append("system:market_missing_after_close")
                    self._log(
                        "WARNING",
                        "Resolve kuyruğundaki market okunamadi, resmi sonuc bekleniyor",
                        slug=trade.slot_slug,
                        overdue_seconds=int(overdue),
                    )
                if trade.mode == "live" and overdue > int(self.config.live_max_resolution_delay_seconds):
                    self._set_live_suspension("resolution_market_unavailable")
                continue
            if trade_market.resolved_outcome:
                self._settle_trade(trade, trade_market)
                continue
            if trade_market.closed:
                outcome = self._resolve_outcome_from_market(trade_market, strict=(trade.mode == "live"))
                if outcome:
                    trade_market.resolved_outcome = outcome
                    self._settle_trade(trade, trade_market)
                continue

    def _manage_active_trade(self, current_market: MarketSnapshot | None) -> None:
        if not self.active_trade:
            return
        overdue = self._trade_overdue_seconds(self.active_trade)
        trade_market = current_market if current_market and current_market.slug == self.active_trade.slot_slug else self.public_client.get_market_by_slug(self.active_trade.slot_slug)
        if overdue > 0 and (
            not trade_market
            or (not trade_market.closed and not trade_market.resolved_outcome)
        ):
            official_market = self._official_resolution_market(self.active_trade)
            if official_market and (official_market.closed or official_market.resolved_outcome):
                trade_market = official_market
        if not trade_market:
            try:
                if overdue > 0:
                    if self.active_trade.mode == "live" and overdue > int(self.config.live_max_resolution_delay_seconds):
                        self._set_live_suspension("resolution_market_unavailable")
                    if "system:market_missing_after_close" not in self.active_trade.notes:
                        self.active_trade.notes.append("system:market_missing_after_close")
                        self._log(
                            "WARNING",
                            "Market slug okunamadi, resmi resolve bekleniyor",
                            slug=self.active_trade.slot_slug,
                            overdue_seconds=int(overdue),
                        )
                    self._queue_pending_resolution(self.active_trade, notify=True)
            except Exception:
                pass
            return
        if self.active_trade.mode == "live":
            self._sync_live_position(self.active_trade)
            if self.config.live_require_synced_position and not self.active_trade.synced_size:
                sync_age = self._trade_age_seconds(self.active_trade)
                if sync_age >= int(self.config.live_position_sync_timeout_seconds):
                    self._set_live_suspension("position_sync_timeout")
            if overdue > int(self.config.live_max_resolution_delay_seconds) and not trade_market.resolved_outcome:
                self._set_live_suspension("resolution_delay")
        if trade_market.resolved_outcome:
            self._settle_trade(self.active_trade, trade_market)
            return
        if trade_market.closed:
            outcome = self._resolve_outcome_from_market(trade_market, strict=(self.active_trade.mode == "live"))
            if outcome:
                trade_market.resolved_outcome = outcome
                self._settle_trade(self.active_trade, trade_market)
            else:
                self._queue_pending_resolution(self.active_trade, notify=True)
            return
        # Emergency exit — yalnız canlı: felaket fiyat hareketinde kısmi kurtarma
        if self.active_trade.mode == "live":
            live_side = trade_market.side(self.active_trade.side)
            if 0 < live_side.bid < self.config.live_emergency_exit_bid:
                self._log(
                    "WARNING",
                    "Live emergency exit tetiklendi",
                    slug=trade_market.slug,
                    bid=live_side.bid,
                    threshold=self.config.live_emergency_exit_bid,
                )
                self._close_trade(trade_market, "emergency_exit")
                return
        if self.config.hold_until_resolution:
            return
        side = trade_market.side(self.active_trade.side)
        move = round(side.bid - self.active_trade.entry_price, 4)
        end_dt = datetime.fromisoformat(trade_market.end_time.replace("Z", "+00:00"))
        seconds_to_close = int(max(0, end_dt.timestamp() - datetime.now(UTC).timestamp()))
        if move >= self.config.take_profit_delta:
            self._close_trade(trade_market, "take_profit")
        elif move <= -self.config.stop_loss_delta:
            self._close_trade(trade_market, "stop_loss")

    def _run_cycle(self) -> dict[str, Any]:
        with self.lock:
            self.cycle_index += 1
            cycle_index = self.cycle_index
            self.last_cycle_started_at = utc_now_iso()
            self._cycle_live_shadow_cache_key = ""
            self._cycle_live_shadow_cache_result = {}
            persist_signature_before = self._persistence_signature()
        started = time.monotonic()
        cycle_row: dict[str, Any]
        try:
            market = self.public_client.get_current_market()
            if market is None:
                raise RuntimeError("aktif BTC 5m market bulunamadı")

            # Resubscribe Polymarket WS when active market token_ids change (new 5m slot)
            token_ids = [t for t in [market.up.token_id, market.down.token_id] if t]
            if token_ids != self._last_ws_token_ids:
                self.poly_ws.resubscribe(token_ids)
                self._last_ws_token_ids = token_ids

            # Overlay real-time bid/ask from Polymarket WS if available
            market = self._overlay_ws_prices(market)

            # Prefer WS context (tick-level) over REST polling when ready
            if self.binance_ws.is_ready():
                btc_context = self.binance_ws.get_context(self.config.signal_lookback_minutes)
            else:
                btc_context = self.btc_client.fetch_context(self.config.signal_lookback_minutes)
                ws_context = self.binance_ws.get_context(self.config.signal_lookback_minutes)
                if safe_float(ws_context.get("price")) > 0:
                    btc_context["price"] = ws_context["price"]
                    btc_context["trade_momentum"] = ws_context.get("trade_momentum", {})
                    btc_context["source"] = f"{btc_context.get('source', 'REST')} + Binance WS momentum"

            signal = self.signal_engine.build_signal(market, btc_context)

            with self.lock:
                self.last_market = market
                self.last_signal = signal

            if cycle_index == 1 or cycle_index % 18 == 0:
                try:
                    with self.lock:
                        self.last_geoblock = self.public_client.check_geoblock()
                except Exception as exc:
                    self._log("ERROR", "Geoblock kontrolü başarısız", error=str(exc))

            effective_mode = self._effective_mode()

            if effective_mode == "live" and self.live_client:
                try:
                    with self.lock:
                        self.last_collateral_status = self.live_client.collateral_status()
                except Exception as exc:
                    self._log("ERROR", "Collateral durumu alınamadı", error=str(exc))

            with self.lock:
                effective_mode = self._maybe_log_mode_transition()

                # Yeni slot başladıysa eski aktif trade'i hemen kapat.
                # Resmi on-chain resolve beklemeden kapanış fiyatından sonucu türet.
                if (
                    self.active_trade
                    and self.active_trade.status == "open"
                    and self.active_trade.slot_slug != market.slug
                ):
                    try:
                        old_market = self.public_client.get_market_by_slug(self.active_trade.slot_slug)
                    except Exception:
                        old_market = None
                    if old_market:
                        outcome = self._resolve_outcome_from_market(old_market, strict=(self.active_trade.mode == "live"))
                        if outcome:
                            old_market.resolved_outcome = outcome
                            self._settle_trade(self.active_trade, old_market)
                        else:
                            self._queue_pending_resolution(self.active_trade, notify=True)
                    else:
                        self._queue_pending_resolution(self.active_trade, notify=True)

                self._manage_active_trade(market)
                self._manage_pending_resolution_trades(market)
                self._manage_pending_shadow_market_reports(market)
                allowed, reason = self._entry_permitted(signal, market)
                self._maybe_send_market_shadow_report(market, signal, allowed, reason)

                if allowed:
                    try:
                        self._open_trade(market, signal)
                    except Exception as exc:
                        allowed = False
                        error_text = str(exc)
                        if error_text.startswith("paper_live_shadow_failed:"):
                            reason = error_text
                            self._log("INFO", "Paper pozisyon live shadow filtresinden geçmedi", reason=reason, slug=market.slug)
                        elif effective_mode == "live":
                            reason = "live_order_failed"
                            self._mark_live_order_failure(exc)
                        else:
                            reason = "trade_open_failed"
                            self._log("ERROR", "Pozisyon acilamadi", error=str(exc), slug=market.slug)
                if not allowed and (signal.reason != "ok" or reason != "open_trade_exists"):
                    self._log("INFO", "Yeni giriş atlandı", reason=reason, slug=market.slug)

                effective_mode = self._maybe_log_mode_transition()
                equity = self._paper_equity(market) if effective_mode == "paper" else self._live_equity()
                cycle_row = {
                    "cycle": cycle_index,
                    "ts": utc_now_iso(),
                    "duration_ms": round((time.monotonic() - started) * 1000.0, 2),
                    "mode": effective_mode,
                    "target_mode": self.config.trading_mode,
                    "paper_grace_remaining_seconds": self._paper_grace_remaining_seconds(),
                    "slot_slug": market.slug,
                    "signal_side": signal.direction,
                    "signal_confidence": signal.confidence,
                    "expected_edge": signal.expected_edge,
                    "equity_usdc": equity,
                    "open_trade": bool(self.active_trade),
                    "pending_resolution_count": len(self.pending_resolution_trades),
                }
                self.recent_cycles.append(cycle_row)
                self.recent_cycles = self.recent_cycles[-40:]
                self.last_error = None
                self._maybe_send_hourly_report()
        except Exception as exc:
            with self.lock:
                self.last_error = str(exc)
                self._log("ERROR", "Cycle hata verdi", error=str(exc), cycle=cycle_index)
                cycle_row = {
                    "cycle": cycle_index,
                    "ts": utc_now_iso(),
                    "duration_ms": round((time.monotonic() - started) * 1000.0, 2),
                    "error": self.last_error,
                }
                self.recent_cycles.append(cycle_row)
                self.recent_cycles = self.recent_cycles[-40:]
        finally:
            with self.lock:
                self.last_cycle_finished_at = utc_now_iso()
                state_changed = self._persistence_signature() != persist_signature_before or bool(self.last_error)
                self._persist_state_if_due(force=state_changed)
        return cycle_row

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            self._run_cycle()
            self.stop_event.wait(self._loop_wait_seconds())

    def start(self) -> dict[str, Any]:
        with self.lock:
            if self.thread and self.thread.is_alive():
                return {"ok": True, "message": "Bot zaten çalışıyor"}
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._loop, name="polymarket-bot-loop", daemon=True)
            self.thread.start()
            self._log("INFO", "Bot başlatıldı")
        return {"ok": True, "message": "Bot başlatıldı"}

    def stop(self) -> dict[str, Any]:
        with self.lock:
            thread = self.thread
            self.stop_event.set()
        if thread and thread.is_alive():
            thread.join(timeout=5)
        with self.lock:
            self.thread = None
            self._log("INFO", "Bot durduruldu")
            self._persist_state()
        return {"ok": True, "message": "Bot durduruldu"}

    def close(self) -> None:
        self.telegram_command_stop.set()
        self.stop()
        self.binance_ws.stop()
        self.poly_ws.stop()
        self.notifier.close()
        self.public_client.close()
        self.btc_client.close()
        command_thread = self.telegram_command_thread
        if command_thread and command_thread.is_alive() and command_thread is not threading.current_thread():
            command_thread.join(timeout=3)

    def run_once(self) -> dict[str, Any]:
        return self._run_cycle()

    def reset_state(self) -> dict[str, Any]:
        with self.lock:
            self.active_trade = None
            self.pending_resolution_trades = []
            self.closed_trades = []
            self.log_store.clear_logs()
            self.log_store.clear_trades()
            self.recent_cycles = []
            self.last_signal = None
            self.last_market = None
            self.last_live_shadow = {}
            self.last_geoblock = {}
            self.last_collateral_status = {}
            self.last_live_positions = []
            self.last_traded_slot_slug = ""
            self.last_market_shadow_notify_slug = ""
            self.last_paper_shadow_skip_notify_key = ""
            self.pending_shadow_market_reports = []
            self.shadow_comparison_history = []
            self.paper_cash_usdc = self.config.paper_balance_usdc
            self.cycle_index = 0
            self.last_cycle_started_at = ""
            self.last_cycle_finished_at = ""
            self.last_error = None
            self.live_rollout_started_at = ""
            self.last_hourly_report_key = ""
            self.last_hourly_report_sent_at = ""
            self.pnl_tracking_started_at = utc_now_iso()
            self._clear_live_suspension()
            self._ensure_rollout_anchor()
            self.last_effective_mode = self._effective_mode()
            self._persist_state()
        return {"ok": True, "message": "State temizlendi"}

    def reset_balance(self) -> dict[str, Any]:
        with self.lock:
            self.paper_cash_usdc = self.config.paper_balance_usdc
            if self.active_trade and self.active_trade.mode == "paper":
                self.active_trade = None
            self.pending_resolution_trades = [trade for trade in self.pending_resolution_trades if trade.mode != "paper"]
            if self._effective_mode() == "paper":
                self.closed_trades = []
                self.log_store.clear_logs()
                self.log_store.clear_trades()
                self.pnl_tracking_started_at = utc_now_iso()
                self.last_hourly_report_key = ""
                self.last_hourly_report_sent_at = ""
            self._clear_live_suspension()
            self._persist_state()
        return {"ok": True, "message": "Paper bakiye sıfırlandı"}

    def reset_pnl(self) -> dict[str, Any]:
        with self.lock:
            self.closed_trades = []
            self.log_store.clear_logs()
            self.log_store.clear_trades()
            self.recent_cycles = []
            self.pnl_tracking_started_at = utc_now_iso()
            self.last_hourly_report_key = ""
            self.last_hourly_report_sent_at = ""
            self._persist_state()
            self._log("INFO", "PnL sayaçları sıfırlandı", pnl_tracking_started_at=self.pnl_tracking_started_at)
            self._notify(
                "PnL sayaçları sıfırlandı.\n"
                f"Yeni PnL başlangıcı: {self._fmt_local_time(self.pnl_tracking_started_at)}\n"
                "Saatlik Telegram raporları bundan sonraki işlemleri gösterecek.",
                respect_trade_toggle=False,
            )
        return {
            "ok": True,
            "message": "PnL sayaçları sıfırlandı",
            "pnl_tracking_started_at": self.pnl_tracking_started_at,
        }

    def update_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            previous_target_mode = self.config.trading_mode
            for name in BotConfig.__dataclass_fields__:
                if name not in payload or name in {"state_file", "secrets_file"}:
                    continue
                current = getattr(self.config, name)
                value = payload[name]
                if isinstance(current, bool):
                    setattr(self.config, name, coerce_bool(value))
                elif isinstance(current, int):
                    setattr(self.config, name, int(float(value)))
                elif isinstance(current, float):
                    setattr(self.config, name, float(value))
                else:
                    setattr(self.config, name, str(value))

            self.config.trading_mode = "live" if str(self.config.trading_mode).strip().lower() == "live" else "paper"
            self.config.paper_grace_minutes = max(0, min(int(self.config.paper_grace_minutes), 1440))
            self.config.poll_seconds = max(1, min(int(self.config.poll_seconds), 60))
            self.config.resolution_poll_seconds = max(1, min(int(self.config.resolution_poll_seconds), 5))
            self.config.fast_poll_window_seconds = max(5, min(int(self.config.fast_poll_window_seconds), 120))
            self.config.signal_lookback_minutes = max(20, min(int(self.config.signal_lookback_minutes), 120))
            self.config.paper_balance_usdc = max(25.0, float(self.config.paper_balance_usdc))
            self.config.order_size_usdc = max(5.0, float(self.config.order_size_usdc))
            self.config.high_confidence_threshold = max(50.0, min(float(self.config.high_confidence_threshold), 99.0))
            self.config.high_confidence_order_size_usdc = max(
                self.config.order_size_usdc,
                min(float(self.config.high_confidence_order_size_usdc), 100000.0),
            )
            self.config.min_confidence = max(50.0, min(float(self.config.min_confidence), 99.0))
            self.config.min_edge = max(0.0, min(float(self.config.min_edge), 0.5))
            self.config.min_entry_price = max(0.0, min(float(getattr(self.config, "min_entry_price", 0.0)), 0.49))
            self.config.max_entry_price = max(0.05, min(float(self.config.max_entry_price), 0.95))
            self.config.momentum_max_entry_price = max(
                self.config.max_entry_price,
                min(float(self.config.momentum_max_entry_price), 0.95),
            )
            self.config.min_entry_seconds = max(0, min(int(self.config.min_entry_seconds), 260))
            self.config.max_entry_seconds = max(20, min(int(self.config.max_entry_seconds), 285))
            if self.config.max_entry_seconds <= self.config.min_entry_seconds:
                self.config.max_entry_seconds = min(285, self.config.min_entry_seconds + 15)
            self.config.min_seconds_left_to_trade = max(5, min(int(self.config.min_seconds_left_to_trade), 240))
            self.config.signal_min_alignment_score = max(0.0, min(float(self.config.signal_min_alignment_score), 1.0))
            self.config.signal_late_min_alignment_score = max(0.0, min(float(self.config.signal_late_min_alignment_score), 1.0))
            self.config.signal_late_reversal_seconds = max(0, min(int(self.config.signal_late_reversal_seconds), 285))
            self.config.signal_min_efficiency_ratio = max(0.0, min(float(self.config.signal_min_efficiency_ratio), 1.0))
            self.config.signal_min_ofi_volume_btc = max(0.0, min(float(self.config.signal_min_ofi_volume_btc), 250.0))
            self.config.paper_require_live_shadow = bool(self.config.paper_require_live_shadow)
            self.config.live_min_confidence = max(50.0, min(float(self.config.live_min_confidence), 99.0))
            self.config.live_min_edge = max(0.0, min(float(self.config.live_min_edge), 0.5))
            self.config.live_min_fee_adj_pnl_usdc = max(-5.0, min(float(self.config.live_min_fee_adj_pnl_usdc), 5.0))
            self.config.live_min_liquidity_usdc = max(0.0, min(float(self.config.live_min_liquidity_usdc), 500000.0))
            self.config.live_max_spread = max(0.0, min(float(self.config.live_max_spread), 0.25))
            self.config.live_signal_confirmations = max(1, min(int(self.config.live_signal_confirmations), 6))
            self.config.live_max_daily_loss_usdc = max(1.0, min(float(self.config.live_max_daily_loss_usdc), 100000.0))
            self.config.live_max_consecutive_losses = max(1, min(int(self.config.live_max_consecutive_losses), 20))
            self.config.live_max_order_failures = max(1, min(int(self.config.live_max_order_failures), 20))
            self.config.live_max_resolution_delay_seconds = max(15, min(int(self.config.live_max_resolution_delay_seconds), 3600))
            self.config.live_position_sync_timeout_seconds = max(5, min(int(self.config.live_position_sync_timeout_seconds), 300))
            self.config.live_entry_price_buffer = max(0.0, min(float(self.config.live_entry_price_buffer), 0.20))
            self.config.live_orderbook_depth_multiplier = max(1.0, min(float(self.config.live_orderbook_depth_multiplier), 10.0))
            self.config.live_orderbook_max_age_seconds = max(0.5, min(float(self.config.live_orderbook_max_age_seconds), 10.0))
            self.config.binance_ws_max_age_seconds = max(1.0, min(float(self.config.binance_ws_max_age_seconds), 60.0))
            self.config.polymarket_ws_max_age_seconds = max(1.0, min(float(self.config.polymarket_ws_max_age_seconds), 60.0))
            self.config.take_profit_delta = max(0.01, min(float(self.config.take_profit_delta), 0.5))
            self.config.stop_loss_delta = max(0.01, min(float(self.config.stop_loss_delta), 0.5))
            self.config.market_order_buffer = max(0.0, min(float(self.config.market_order_buffer), 0.20))
            self.config.paper_taker_fee_rate = max(0.0, min(float(self.config.paper_taker_fee_rate), 0.20))
            self.config.request_timeout_sec = max(5.0, min(float(self.config.request_timeout_sec), 60.0))

            self.public_client = PolymarketPublicClient(self.config)
            self.btc_client = BinanceBtcClient(self.config.price_source_base_url, timeout=self.config.request_timeout_sec)
            self.signal_engine = SignalEngine(self.config)
            self.notifier.timeout = self.config.request_timeout_sec
            self.live_client = self._build_live_client()
            if self.config.trading_mode == "live" and previous_target_mode != "live":
                self.live_rollout_started_at = utc_now_iso()
                self._reset_live_confirmation()
                self._log("INFO", "Live rollout arm edildi", paper_grace_minutes=self.config.paper_grace_minutes)
            elif self.config.trading_mode != "live":
                self.live_rollout_started_at = ""
                self._clear_live_suspension()
            self._ensure_rollout_anchor()
            self.last_effective_mode = self._effective_mode()
            self._persist_state()
        return {"ok": True, "message": "Bot ayarları güncellendi", "config": asdict(self.config)}

    def update_secrets(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self.secrets = self.secret_store.update(payload).apply_env_overrides()
            self.notifier.configure(
                token=self.secrets.telegram_bot_token,
                chat_id=self.secrets.telegram_chat_id,
                base_url=self.secrets.telegram_api_base_url,
            )
            self.live_client = self._build_live_client()
            self._persist_state()
        self._start_telegram_commands()
        return {"ok": True, "message": "Secrets güncellendi"}

    def test_telegram(self) -> dict[str, Any]:
        if not self.notifier.configured:
            return {"ok": False, "message": "Telegram secrets eksik"}
        self.notifier.send_message(self._format_hourly_report())
        return {"ok": True, "message": "Telegram test mesajı gönderildi"}

    def test_polymarket(self) -> dict[str, Any]:
        market = self.public_client.get_current_market()
        geo = self.public_client.check_geoblock()
        if not market:
            return {"ok": False, "message": "Aktif BTC 5m market bulunamadı", "geoblock": geo}
        response: dict[str, Any] = {
            "ok": True,
            "message": "Polymarket bağlantısı hazır",
            "market": {"slug": market.slug, "question": market.question, "up_ask": market.up.ask, "down_ask": market.down.ask},
            "geoblock": geo,
        }
        if self.live_client and self.live_client.configured:
            response["wallet"] = self.live_client.wallet_address
            response["profile_address"] = self.live_client.profile_address
            response["collateral"] = self.live_client.collateral_status()
            response["health"] = {
                "geoblock_ok": not bool(geo.get("blocked")),
                "polymarket_secret_ok": bool(self.live_client.configured),
                "collateral_ok": safe_float(response["collateral"].get("balance")) >= self._max_order_size_usdc(),
                "binance_ws_fresh": self.binance_ws.is_fresh(float(self.config.binance_ws_max_age_seconds)),
                "polymarket_ws_connected": bool(self.poly_ws.status([market.up.token_id, market.down.token_id]).get("connected")),
                "live_reprice_before_order": bool(self.config.live_reprice_before_order),
            }
        return response

    def get_state(self) -> dict[str, Any]:
        with self.lock:
            effective_mode = self._effective_mode()
            grace_remaining_seconds = self._paper_grace_remaining_seconds()
            cycle_analytics = self._cycle_analytics()
            equity = self._paper_equity(self.last_market) if effective_mode == "paper" else self._live_equity()
            masked = self.secrets.masked()
            masked["file"] = self.config.secrets_file
            summary = {
                "mode": effective_mode,
                "target_mode": self.config.trading_mode,
                "rollout_status": self._rollout_status(),
                "paper_grace_minutes": int(self.config.paper_grace_minutes),
                "paper_grace_remaining_seconds": grace_remaining_seconds,
                "paper_grace_started_at": self.live_rollout_started_at,
                "paper_cash_usdc": round(self.paper_cash_usdc, 8),
                "current_equity_usdc": equity,
                "paper_equity_usdc": self._paper_equity(self.last_market),
                "live_equity_usdc": self._live_equity(),
                "realized_pnl_usdc": self._realized_pnl(),
                "pnl_tracking_started_at": self.pnl_tracking_started_at,
                "win_rate_pct": self._win_rate_pct(),
                "open_trade_count": (1 if self.active_trade else 0) + len(self.pending_resolution_trades),
                "active_trade_count": 1 if self.active_trade else 0,
                "pending_resolution_count": len(self.pending_resolution_trades),
                "last_traded_slot_slug": self.last_traded_slot_slug,
                "live_suspended_reason": self.live_suspended_reason,
                "live_suspended_at": self.live_suspended_at,
                "live_order_failures": int(self.live_order_failures),
                "live_daily_pnl_usdc": self._live_daily_pnl(),
                "live_consecutive_losses": self._live_consecutive_losses(),
            }
            if cycle_analytics.get("best_cycle"):
                summary["best_cycle_label"] = cycle_analytics["best_cycle"]["label"]
                summary["best_cycle_range"] = cycle_analytics["best_cycle"]["cycle_range"]
            if self.config.trading_mode == "live":
                summary["collateral_balance_usdc"] = self._live_collateral_balance()
                summary["open_positions_value_usdc"] = round(sum(safe_float(item.get("currentValue")) for item in self.last_live_positions), 8)
            return {
                "summary": summary,
                "runtime": {
                    "running": bool(self.thread and self.thread.is_alive()),
                    "cycle_index": self.cycle_index,
                    "state_file": self.config.state_file,
                    "last_cycle_started_at": self.last_cycle_started_at,
                    "last_cycle_finished_at": self.last_cycle_finished_at,
                    "last_error": self.last_error,
                    "live_rollout_started_at": self.live_rollout_started_at,
                    "live_sdk_package": self.live_sdk_package,
                    "last_live_shadow": self.last_live_shadow,
                    "pending_shadow_market_reports": self.pending_shadow_market_reports[-10:],
                    "shadow_comparison_history": self.shadow_comparison_history[-10:],
                    "feeds": self._feed_status(self.last_market),
                },
                "market": {
                    "current_market": asdict(self.last_market) if self.last_market else None,
                    "last_signal": asdict(self.last_signal) if self.last_signal else None,
                    "geoblock": self.last_geoblock,
                    "collateral": self.last_collateral_status,
                    "live_positions": self.last_live_positions[-10:],
                },
                "portfolio": {
                    "active_trade": asdict(self.active_trade) if self.active_trade else None,
                    "pending_resolution_trades": [asdict(item) for item in self.pending_resolution_trades[-20:]],
                    "closed_trades": [asdict(item) for item in self.closed_trades[-20:]],
                },
                "notifications": {
                    "telegram": {
                        "configured": self.notifier.configured,
                        "chat_id_hint": self.notifier.chat_id_hint,
                        "command_listener_running": bool(
                            self.telegram_command_thread and self.telegram_command_thread.is_alive()
                        ),
                        "command_last_error": self.telegram_command_last_error,
                        "trade_notifications_enabled": bool(self.config.telegram_trade_notifications),
                        "hourly_reports_enabled": bool(self.config.telegram_hourly_reports),
                        "last_hourly_report_sent_at": self.last_hourly_report_sent_at,
                        "next_hourly_report_due_at": self._next_local_hour_at(),
                    }
                },
                "config": asdict(self.config),
                "secrets": masked,
                "analytics": cycle_analytics,
                "logs": self.log_store.log_dicts(60),
                "recent_cycles": self.recent_cycles[-20:],
            }
