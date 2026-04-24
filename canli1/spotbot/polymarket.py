from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import requests
from eth_account import Account

try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams, MarketOrderArgs, OrderType
    from py_clob_client_v2.order_builder.constants import BUY, SELL

    CLOB_V2_IMPORT_ERROR = ""
except ImportError as exc:  # pragma: no cover - lets paper mode keep running if SDK is absent
    ClobClient = None  # type: ignore[assignment]
    AssetType = BalanceAllowanceParams = MarketOrderArgs = OrderType = None  # type: ignore[assignment]
    BUY = "BUY"
    SELL = "SELL"
    CLOB_V2_IMPORT_ERROR = str(exc)

from .models import BotConfig, MarketSide, MarketSnapshot, PredictionSnapshot
from .utils import clamp, pct_change, safe_float, utc_now_iso


UTC = timezone.utc


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _ema(values: list[float], span: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (max(2, span) + 1.0)
    result = values[0]
    for value in values[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def _slot_floor(now: datetime) -> datetime:
    epoch = int(now.timestamp())
    floored = epoch - (epoch % 300)
    return datetime.fromtimestamp(floored, UTC)


class _RequestsMixin:
    def __init__(self, timeout: float = 15.0) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "PolymarketBTC5mBot/1.0"})

    def close(self) -> None:
        self.session.close()

    def _get_json(self, url: str, params: dict[str, Any] | None = None, allow_404: bool = False) -> Any:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= 2:
                    raise RuntimeError(str(exc)) from exc
                time.sleep(0.25 * (attempt + 1))
                continue
            if allow_404 and response.status_code == 404:
                return None
            if response.ok:
                return response.json()
            if response.status_code >= 500 and attempt < 2:
                time.sleep(0.25 * (attempt + 1))
                continue
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            raise RuntimeError(f"HTTP {response.status_code}: {detail}")
        raise RuntimeError(str(last_error or "request failed"))


class BinanceBtcClient(_RequestsMixin):
    def __init__(self, base_url: str, timeout: float = 15.0) -> None:
        super().__init__(timeout)
        self.base_url = base_url.rstrip("/")

    def fetch_context(self, lookback_minutes: int) -> dict[str, Any]:
        limit = max(20, min(int(lookback_minutes), 120))
        ticker = self._get_json(f"{self.base_url}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
        klines = self._get_json(
            f"{self.base_url}/api/v3/klines",
            {"symbol": "BTCUSDT", "interval": "1m", "limit": limit},
        )
        rows: list[dict[str, float]] = []
        for row in klines:
            rows.append(
                {
                    "open_time": safe_float(row[0]),
                    "open": safe_float(row[1]),
                    "high": safe_float(row[2]),
                    "low": safe_float(row[3]),
                    "close": safe_float(row[4]),
                    "volume": safe_float(row[5]),
                }
            )
        return {
            "price": safe_float(ticker.get("price")),
            "candles": rows,
            "source": "Binance BTCUSDT 1m + ticker",
        }


class PolymarketPublicClient(_RequestsMixin):
    def __init__(self, config: BotConfig) -> None:
        super().__init__(config.request_timeout_sec)
        self.config = config
        self.gamma_base_url = config.gamma_base_url.rstrip("/")
        self.clob_base_url = config.clob_base_url.rstrip("/")
        self.data_base_url = config.data_base_url.rstrip("/")

    def current_slot_start(self, now: datetime | None = None) -> datetime:
        return _slot_floor(now or datetime.now(UTC))

    def slot_slug(self, slot_start: datetime) -> str:
        return f"btc-updown-5m-{int(slot_start.timestamp())}"

    def get_market_by_slug(self, slug: str) -> MarketSnapshot | None:
        raw = self._get_json(f"{self.gamma_base_url}/markets/slug/{slug}", allow_404=True)
        if not raw:
            return None
        return self._normalize_market(raw)

    def get_clob_market(self, condition_id: str) -> MarketSnapshot | None:
        market_id = str(condition_id or "").strip()
        if not market_id:
            return None
        raw = self._get_json(f"{self.clob_base_url}/markets/{market_id}", allow_404=True)
        if not raw:
            return None
        return self._normalize_clob_market(raw)

    @staticmethod
    def _book_row_price_size(row: Any) -> tuple[float, float]:
        if isinstance(row, dict):
            return safe_float(row.get("price")), safe_float(row.get("size"))
        if isinstance(row, list) and len(row) >= 2:
            return safe_float(row[0]), safe_float(row[1])
        return 0.0, 0.0

    def get_order_book_quote(self, token_id: str, limit_price: float | None = None) -> dict[str, float]:
        token = str(token_id or "").strip()
        if not token:
            return {
                "bid": 0.0,
                "ask": 0.0,
                "mid": 0.0,
                "spread": 0.0,
                "ask_depth_usdc": 0.0,
                "ask_depth_shares": 0.0,
                "ask_depth_levels": 0.0,
                "depth_limit_price": 0.0,
                "book_age_seconds": 999999.0,
            }
        raw = self._get_json(f"{self.clob_base_url}/book", {"token_id": token})
        bids = raw.get("bids", []) if isinstance(raw, dict) else []
        asks = raw.get("asks", []) if isinstance(raw, dict) else []

        bid_rows = [self._book_row_price_size(row) for row in bids]
        ask_rows = [self._book_row_price_size(row) for row in asks]
        best_bid = max((price for price, _ in bid_rows), default=0.0)
        best_ask = min((price for price, _ in ask_rows if price > 0), default=0.0)
        mid = round((best_bid + best_ask) / 2.0, 4) if best_bid > 0 and best_ask > 0 else 0.0
        spread = round(best_ask - best_bid, 6) if best_bid > 0 and best_ask > 0 else 0.0
        depth_limit = safe_float(limit_price, best_ask)
        if depth_limit <= 0:
            depth_limit = best_ask
        ask_depth_shares = 0.0
        ask_depth_usdc = 0.0
        ask_depth_levels = 0
        for price, size in ask_rows:
            if price <= 0 or size <= 0:
                continue
            if price <= depth_limit + 1e-9:
                ask_depth_levels += 1
                ask_depth_shares += size
                ask_depth_usdc += price * size
        timestamp_ms = safe_float(raw.get("timestamp")) if isinstance(raw, dict) else 0.0
        book_age = time.time() - timestamp_ms / 1000.0 if timestamp_ms > 0 else 999999.0
        return {
            "bid": round(best_bid, 4),
            "ask": round(best_ask, 4),
            "mid": mid,
            "spread": spread,
            "ask_depth_usdc": round(ask_depth_usdc, 6),
            "ask_depth_shares": round(ask_depth_shares, 6),
            "ask_depth_levels": float(ask_depth_levels),
            "depth_limit_price": round(depth_limit, 4),
            "book_age_seconds": round(max(0.0, book_age), 3),
        }

    def get_order_book_top(self, token_id: str) -> dict[str, float]:
        quote = self.get_order_book_quote(token_id)
        return {
            "bid": quote["bid"],
            "ask": quote["ask"],
            "mid": quote["mid"],
            "spread": quote["spread"],
        }

    def get_current_market(self, now: datetime | None = None) -> MarketSnapshot | None:
        current_slot = self.current_slot_start(now)
        candidates = [current_slot, current_slot - timedelta(minutes=5), current_slot + timedelta(minutes=5)]
        snapshots: list[MarketSnapshot] = []
        for slot_start in candidates:
            market = self.get_market_by_slug(self.slot_slug(slot_start))
            if market is not None:
                snapshots.append(market)
        if not snapshots:
            return None
        for market in snapshots:
            if market.accepting_orders and not market.closed:
                return market
        for market in snapshots:
            if not market.closed:
                return market
        return snapshots[-1]

    def check_geoblock(self) -> dict[str, Any]:
        payload = self._get_json("https://polymarket.com/api/geoblock")
        return {
            "blocked": bool(payload.get("blocked", False)),
            "country": str(payload.get("country", "")),
            "region": str(payload.get("region", "")),
            "checked_at": utc_now_iso(),
        }

    def get_positions(self, user: str, condition_id: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "user": user,
            "limit": 100,
            "sizeThreshold": 0,
            "sortBy": "TITLE",
            "sortDirection": "DESC",
        }
        if condition_id:
            params["market"] = condition_id
        payload = self._get_json(f"{self.data_base_url}/positions", params=params)
        return payload if isinstance(payload, list) else []

    def _normalize_market(self, raw: dict[str, Any]) -> MarketSnapshot:
        outcomes = [str(item) for item in _json_list(raw.get("outcomes"))]
        prices = [round(clamp(safe_float(item), 0.0, 1.0), 4) for item in _json_list(raw.get("outcomePrices"))]
        token_ids = [str(item) for item in _json_list(raw.get("clobTokenIds"))]
        up_price = prices[0] if len(prices) >= 1 else 0.5
        down_price = prices[1] if len(prices) >= 2 else round(1.0 - up_price, 4)
        tick_size = max(safe_float(raw.get("orderPriceMinTickSize"), 0.01), 0.01)
        spread = max(safe_float(raw.get("spread"), tick_size), tick_size)
        best_bid = round(clamp(safe_float(raw.get("bestBid")), 0.0, 1.0), 4)
        best_ask = round(clamp(safe_float(raw.get("bestAsk")), 0.0, 1.0), 4)
        closed = bool(raw.get("closed", False))
        accepting_orders = bool(raw.get("acceptingOrders", False))
        if closed and not accepting_orders and up_price in {0.0, 1.0} and down_price in {0.0, 1.0}:
            up_bid = up_ask = up_price
            down_bid = down_ask = down_price
        else:
            up_bid = best_bid if best_bid > 0 else round(clamp(up_price - spread / 2.0, tick_size, 1.0 - tick_size), 4)
            up_ask = best_ask if best_ask > 0 else round(clamp(up_price + spread / 2.0, tick_size, 1.0 - tick_size), 4)
            down_bid = round(clamp(1.0 - up_ask, tick_size, 1.0 - tick_size), 4)
            down_ask = round(clamp(1.0 - up_bid, tick_size, 1.0 - tick_size), 4)
        resolved_outcome = ""
        if closed:
            winner = str(raw.get("winner") or raw.get("winningOutcome") or "").strip().upper()
            if winner == "UP":
                resolved_outcome = "UP"
            elif winner == "DOWN":
                resolved_outcome = "DOWN"
            elif up_price >= 0.99 and down_price <= 0.01:
                resolved_outcome = "UP"
            elif down_price >= 0.99 and up_price <= 0.01:
                resolved_outcome = "DOWN"
        return MarketSnapshot(
            slug=str(raw.get("slug", "")),
            question=str(raw.get("question", "")),
            condition_id=str(raw.get("conditionId", "")),
            start_time=str(raw.get("startDate", "")),
            end_time=str(raw.get("endDate", "")),
            active=bool(raw.get("active", False)),
            closed=closed,
            accepting_orders=accepting_orders,
            restricted=bool(raw.get("restricted", False)),
            tick_size=round(tick_size, 4),
            min_order_size=safe_float(raw.get("orderMinSize"), 5.0),
            spread=round(spread, 4),
            best_bid=round(up_bid, 4),
            best_ask=round(up_ask, 4),
            last_trade_price=round(clamp(safe_float(raw.get("lastTradePrice")), 0.0, 1.0), 4),
            volume=safe_float(raw.get("volumeNum"), safe_float(raw.get("volume"))),
            liquidity=safe_float(raw.get("liquidityNum"), safe_float(raw.get("liquidity"))),
            fees_enabled=bool(raw.get("feesEnabled", True)),
            fee_rate=self.config.paper_taker_fee_rate,
            updated_at=str(raw.get("updatedAt", "")),
            up=MarketSide(
                label=outcomes[0] if len(outcomes) >= 1 else "Up",
                token_id=token_ids[0] if len(token_ids) >= 1 else "",
                price=round(up_price, 4),
                bid=round(up_bid, 4),
                ask=round(up_ask, 4),
            ),
            down=MarketSide(
                label=outcomes[1] if len(outcomes) >= 2 else "Down",
                token_id=token_ids[1] if len(token_ids) >= 2 else "",
                price=round(down_price, 4),
                bid=round(down_bid, 4),
                ask=round(down_ask, 4),
            ),
            resolved_outcome=resolved_outcome,
        )

    def _normalize_clob_market(self, raw: dict[str, Any]) -> MarketSnapshot:
        tokens = raw.get("tokens", [])
        token_rows = tokens if isinstance(tokens, list) else []

        def _pick_token(label: str, fallback_index: int) -> dict[str, Any]:
            wanted = label.strip().lower()
            for row in token_rows:
                outcome = str(row.get("outcome", "")).strip().lower()
                if outcome == wanted:
                    return row
            if 0 <= fallback_index < len(token_rows):
                return token_rows[fallback_index]
            return {}

        up_row = _pick_token("up", 0)
        down_row = _pick_token("down", 1 if len(token_rows) > 1 else 0)
        if up_row == down_row and len(token_rows) > 1:
            down_row = token_rows[1]

        up_price = round(clamp(safe_float(up_row.get("price"), 0.5), 0.0, 1.0), 4)
        down_price = round(clamp(safe_float(down_row.get("price"), max(0.0, 1.0 - up_price)), 0.0, 1.0), 4)
        tick_size = max(safe_float(raw.get("minimum_tick_size"), 0.01), 0.01)
        closed = bool(raw.get("closed", False))
        accepting_orders = bool(raw.get("accepting_orders", False))

        resolved_outcome = ""
        if bool(up_row.get("winner")):
            resolved_outcome = "UP"
            up_price, down_price = 1.0, 0.0
        elif bool(down_row.get("winner")):
            resolved_outcome = "DOWN"
            up_price, down_price = 0.0, 1.0

        if closed:
            up_bid = up_ask = up_price
            down_bid = down_ask = down_price
        else:
            up_bid = up_ask = up_price
            down_bid = down_ask = down_price

        return MarketSnapshot(
            slug=str(raw.get("market_slug") or raw.get("slug") or ""),
            question=str(raw.get("question", "")),
            condition_id=str(raw.get("condition_id") or raw.get("conditionId") or ""),
            start_time=str(raw.get("start_date_iso") or raw.get("startDate") or ""),
            end_time=str(raw.get("end_date_iso") or raw.get("endDate") or ""),
            active=bool(raw.get("active", False)),
            closed=closed,
            accepting_orders=accepting_orders,
            restricted=bool(raw.get("restricted", False)),
            tick_size=round(tick_size, 4),
            min_order_size=safe_float(raw.get("minimum_order_size"), 5.0),
            spread=round(tick_size, 4),
            best_bid=round(up_bid, 4),
            best_ask=round(up_ask, 4),
            last_trade_price=round(up_price, 4),
            volume=safe_float(raw.get("volume")),
            liquidity=safe_float(raw.get("liquidity")),
            fees_enabled=True,
            fee_rate=self.config.paper_taker_fee_rate,
            updated_at=utc_now_iso(),
            up=MarketSide(
                label=str(up_row.get("outcome") or "Up"),
                token_id=str(up_row.get("token_id", "")),
                price=round(up_price, 4),
                bid=round(up_bid, 4),
                ask=round(up_ask, 4),
            ),
            down=MarketSide(
                label=str(down_row.get("outcome") or "Down"),
                token_id=str(down_row.get("token_id", "")),
                price=round(down_price, 4),
                bid=round(down_bid, 4),
                ask=round(down_ask, 4),
            ),
            resolved_outcome=resolved_outcome,
        )


class SignalEngine:
    def __init__(self, config: BotConfig) -> None:
        self.config = config

    @staticmethod
    def _market_slot_start(market: MarketSnapshot, timestamp: datetime) -> datetime:
        slug = str(market.slug or "").strip()
        if slug:
            try:
                slot_epoch = int(slug.rsplit("-", 1)[-1])
                if slot_epoch > 0:
                    return datetime.fromtimestamp(slot_epoch, UTC)
            except (TypeError, ValueError):
                pass
        raw = str(market.start_time or "").strip()
        if raw:
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                return parsed.astimezone(UTC)
            except ValueError:
                pass
        return _slot_floor(timestamp)

    @staticmethod
    def _row_price(row: dict[str, Any] | None, *keys: str) -> float:
        if not isinstance(row, dict):
            return 0.0
        for key in keys:
            value = safe_float(row.get(key))
            if value > 0:
                return value
        return 0.0

    def _slot_reference_prices(
        self,
        candles: list[dict[str, Any]],
        slot_start: datetime,
        timestamp: datetime,
    ) -> dict[str, float]:
        rows: dict[int, dict[str, Any]] = {}
        for row in candles:
            open_time_ms = safe_float(row.get("open_time"))
            if open_time_ms <= 0:
                continue
            rows[int(open_time_ms // 1000)] = row

        slot_start_sec = int(slot_start.timestamp())
        current_sec = int(timestamp.timestamp())
        slot_open_row = rows.get(slot_start_sec)
        slot_second_row = rows.get(slot_start_sec + 60)

        slot_open_price = self._row_price(slot_open_row, "open", "close")
        slot_1m_close_price = 0.0
        slot_2m_close_price = 0.0
        if current_sec >= slot_start_sec + 60:
            slot_1m_close_price = self._row_price(slot_open_row, "close", "open")
        if current_sec >= slot_start_sec + 120:
            slot_2m_close_price = self._row_price(slot_second_row, "close", "open")

        return {
            "slot_open_price": slot_open_price,
            "slot_1m_close_price": slot_1m_close_price,
            "slot_2m_close_price": slot_2m_close_price,
        }

    def build_signal(self, market: MarketSnapshot, btc_context: dict[str, Any], now: datetime | None = None) -> PredictionSnapshot:
        timestamp = now or datetime.now(UTC)
        candles = list(btc_context.get("candles") or [])
        current_price = safe_float(btc_context.get("price"))
        closes = [row["close"] for row in candles if row.get("close", 0) > 0]
        highs = [row["high"] for row in candles if row.get("high", 0) > 0]
        lows = [row["low"] for row in candles if row.get("low", 0) > 0]
        volumes = [row["volume"] for row in candles if row.get("volume", 0) >= 0]
        if len(closes) < 20 or current_price <= 0:
            return PredictionSnapshot(
                slug=market.slug,
                direction="UP",
                confidence=50.0,
                fair_up_price=0.5,
                fair_down_price=0.5,
                market_price=market.up.price,
                market_bid=market.up.bid,
                market_ask=market.up.ask,
                expected_edge=0.0,
                strength_score=0.0,
                score=0.0,
                seconds_into_slot=0,
                seconds_to_close=0,
                entry_allowed=False,
                reason="btc_context_unavailable",
                created_at=utc_now_iso(),
            )

        arr_c = np.array(closes, dtype=np.float64)
        arr_h = np.array(highs, dtype=np.float64) if highs else np.array([current_price])
        arr_l = np.array(lows, dtype=np.float64) if lows else np.array([current_price])
        arr_v = np.array(volumes, dtype=np.float64) if volumes else np.zeros(len(closes))
        closes_with_now = np.append(arr_c[:-1], current_price)

        ema_fast = _ema(closes_with_now[-8:].tolist(), 5)
        ema_slow = _ema(closes_with_now[-21:].tolist(), 13)
        ema_gap = pct_change(ema_fast, ema_slow) if ema_slow > 0 else 0.0

        w5h = arr_h[-5:] if len(arr_h) >= 5 else arr_h
        w5l = arr_l[-5:] if len(arr_l) >= 5 else arr_l
        recent_high = float(max(np.max(w5h), current_price))
        recent_low = float(min(np.min(w5l), current_price))
        range_size = max(recent_high - recent_low, max(current_price * 0.00001, 1.0))
        range_position = (current_price - recent_low) / range_size

        w20 = min(20, len(arr_c))
        vwap_c = closes_with_now[-w20:]
        vwap_v = arr_v[-w20:] if len(arr_v) >= w20 else arr_v
        vwap_v = vwap_v[-len(vwap_c):]
        vwap_den = float(np.sum(vwap_v))
        vwap = float(np.dot(vwap_c, vwap_v) / vwap_den) if vwap_den > 0 else current_price
        vwap_gap = pct_change(current_price, vwap) if vwap > 0 else 0.0

        vol_window = arr_v[-12:] if len(arr_v) >= 1 else np.array([1.0])
        volume_avg = float(np.mean(vol_window))
        volume_spike = float(arr_v[-1]) / volume_avg if volume_avg > 0 else 1.0

        momentum = btc_context.get("trade_momentum") or {}
        ofi_30s = safe_float(momentum.get("ofi_30s"))
        ofi_60s = safe_float(momentum.get("ofi_60s"))
        buy_vol_60s = safe_float(momentum.get("buy_vol_60s"))
        sell_vol_60s = safe_float(momentum.get("sell_vol_60s"))
        flow_volume_60s = buy_vol_60s + sell_vol_60s
        flow_quality = clamp(flow_volume_60s / max(float(self.config.signal_min_ofi_volume_btc), 1e-9), 0.0, 1.0)
        book_imbalance = safe_float(momentum.get("book_imbalance"))

        trend_window = closes_with_now[-6:] if len(closes_with_now) >= 6 else closes_with_now
        trend_path = float(np.sum(np.abs(np.diff(trend_window)))) if len(trend_window) >= 2 else 0.0
        trend_distance = abs(float(trend_window[-1] - trend_window[0])) if len(trend_window) >= 2 else 0.0
        efficiency_ratio = clamp(trend_distance / trend_path if trend_path > 0 else 0.0, 0.0, 1.0)
        ret_window = closes_with_now[-8:] if len(closes_with_now) >= 8 else closes_with_now
        ret_series = np.diff(ret_window) / ret_window[:-1] * 100.0 if len(ret_window) >= 2 else np.array([0.0])
        realized_vol_8m = float(np.std(ret_series)) if len(ret_series) else 0.0

        slot_start = self._market_slot_start(market, timestamp)
        seconds_into_slot = int(max(0, timestamp.timestamp() - slot_start.timestamp()))
        try:
            end_dt = datetime.fromisoformat(market.end_time.replace("Z", "+00:00"))
        except ValueError:
            end_dt = slot_start + timedelta(minutes=5)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=UTC)
        end_dt = end_dt.astimezone(UTC)
        seconds_to_close = int(max(0, end_dt.timestamp() - timestamp.timestamp()))

        slot_prices = self._slot_reference_prices(candles, slot_start, timestamp)
        slot_open_price = slot_prices["slot_open_price"]
        slot_1m_close_price = slot_prices["slot_1m_close_price"]
        slot_2m_close_price = slot_prices["slot_2m_close_price"]
        if slot_open_price <= 0:
            return PredictionSnapshot(
                slug=market.slug,
                direction="UP",
                confidence=50.0,
                fair_up_price=0.5,
                fair_down_price=0.5,
                market_price=market.up.price,
                market_bid=market.up.bid,
                market_ask=market.up.ask,
                expected_edge=0.0,
                strength_score=0.0,
                score=0.0,
                seconds_into_slot=seconds_into_slot,
                seconds_to_close=seconds_to_close,
                entry_allowed=False,
                reason="intraslot_reference_missing",
                created_at=utc_now_iso(),
                btc_price=round(current_price, 2),
            )

        slot_ret_live = pct_change(current_price, slot_open_price)
        slot_ret_1m = pct_change(slot_1m_close_price, slot_open_price) if slot_1m_close_price > 0 else 0.0
        slot_ret_2m = pct_change(slot_2m_close_price, slot_open_price) if slot_2m_close_price > 0 else 0.0
        confirmation_stage = 0.0
        confirmed_ret = slot_ret_live
        if seconds_into_slot >= 120 and slot_2m_close_price > 0:
            confirmation_stage = 2.0
            confirmed_ret = slot_ret_2m
        elif seconds_into_slot >= 60 and slot_1m_close_price > 0:
            confirmation_stage = 1.0
            confirmed_ret = slot_ret_1m
        live_extension_pct = slot_ret_live - confirmed_ret
        min_move = max(float(getattr(self.config, "signal_min_intraslot_move_pct", 0.03)), 0.01)
        strong_move = max(float(getattr(self.config, "signal_strong_intraslot_move_pct", 0.05)), min_move)

        direction_bias = confirmed_ret if abs(confirmed_ret) > 1e-9 else slot_ret_live
        if abs(direction_bias) <= 1e-9:
            if abs(ofi_60s) >= 0.10:
                direction = "UP" if ofi_60s >= 0 else "DOWN"
            elif abs(book_imbalance) >= 0.10:
                direction = "UP" if book_imbalance >= 0 else "DOWN"
            else:
                direction = "UP"
        else:
            direction = "UP" if direction_bias >= 0 else "DOWN"
        direction_sign = 1.0 if direction == "UP" else -1.0
        chosen_market = market.side(direction)
        momentum_abs = abs(direction_bias if abs(direction_bias) > 1e-9 else slot_ret_live)

        alignment_inputs = [
            (confirmed_ret, max(min_move * 0.6, 0.015), 1.45),
            (slot_ret_live, max(min_move * 0.6, 0.015), 1.15),
            (live_extension_pct, 0.012, 0.65),
            (ema_gap, 0.010, 0.55),
            (vwap_gap, 0.012, 0.45),
            (ofi_30s if flow_quality >= 0.4 else 0.0, 0.10, 0.70),
            (ofi_60s if flow_quality >= 0.4 else 0.0, 0.12, 0.90),
            (book_imbalance, 0.10, 0.50),
        ]
        total_alignment_weight = sum(item[2] for item in alignment_inputs) or 1.0
        aligned_weight = sum(weight for value, threshold, weight in alignment_inputs if direction_sign * value > threshold)
        counter_weight = sum(weight for value, threshold, weight in alignment_inputs if direction_sign * value < -threshold)
        alignment_score = clamp(aligned_weight / total_alignment_weight, 0.0, 1.0)
        counter_signal_score = clamp(counter_weight / total_alignment_weight, 0.0, 1.0)
        reversal_score = 0.0
        if direction_sign * slot_ret_live < -max(min_move * 0.5, 0.015):
            reversal_score += 0.45
        if direction_sign * live_extension_pct < -0.012:
            reversal_score += 0.25
        if flow_quality >= 0.5 and direction_sign * ofi_30s < -0.20:
            reversal_score += 0.20
        if direction_sign * book_imbalance < -0.18:
            reversal_score += 0.15
        reversal_score = clamp(reversal_score, 0.0, 1.0)

        premium = 0.015
        premium += momentum_abs * 1.55
        premium += max(0.0, direction_sign * live_extension_pct) * 0.45
        premium += max(0.0, direction_sign * ofi_60s) * 0.05
        premium += max(0.0, direction_sign * book_imbalance) * 0.03
        premium += alignment_score * 0.035
        premium += efficiency_ratio * 0.02
        if confirmation_stage >= 2.0 and momentum_abs >= strong_move:
            premium += 0.015
        premium = clamp(premium, 0.02, 0.32)

        if direction == "UP":
            fair_up = clamp(0.5 + premium, 0.05, 0.95)
            fair_down = round(1.0 - fair_up, 4)
            fair_side = fair_up
        else:
            fair_down = clamp(0.5 + premium, 0.05, 0.95)
            fair_up = round(1.0 - fair_down, 4)
            fair_side = fair_down

        ask = chosen_market.ask
        price_progress = max(0.0, ask - 0.5)
        pricing_efficiency = clamp(price_progress / max(fair_side - 0.5, 0.02), 0.0, 2.0)
        raw_confidence = 50.0
        raw_confidence += min(18.0, momentum_abs * 220.0)
        raw_confidence += alignment_score * 18.0
        raw_confidence += 4.0 if confirmation_stage >= 2.0 else 1.5 if confirmation_stage >= 1.0 else 0.0
        confidence_penalty = counter_signal_score * 7.0 + reversal_score * 10.0
        if pricing_efficiency > 0.90:
            confidence_penalty += (pricing_efficiency - 0.90) * 6.0
        if self.config.signal_min_alignment_score > 0 and alignment_score < self.config.signal_min_alignment_score:
            confidence_penalty += (self.config.signal_min_alignment_score - alignment_score) * 14.0
        if self.config.signal_min_efficiency_ratio > 0 and efficiency_ratio < self.config.signal_min_efficiency_ratio:
            confidence_penalty += (self.config.signal_min_efficiency_ratio - efficiency_ratio) * 18.0
        if self.config.signal_late_min_alignment_score > 0:
            confidence_penalty += counter_signal_score * 4.0
            confidence_penalty += reversal_score * 5.0
        if (
            self.config.signal_late_min_alignment_score > 0
            and realized_vol_8m > 0.045
            and alignment_score < self.config.signal_late_min_alignment_score
        ):
            confidence_penalty += min(4.0, (realized_vol_8m - 0.045) * 35.0)
        confidence = clamp(raw_confidence - confidence_penalty, 50.0, 99.0)

        base_max_entry = float(self.config.max_entry_price)
        momentum_max_entry = max(base_max_entry, float(getattr(self.config, "momentum_max_entry_price", base_max_entry)))
        signal_max_entry_price = round(0.5 + premium * 0.72, 4)
        signal_max_entry_price = max(base_max_entry, min(momentum_max_entry, signal_max_entry_price))
        edge = round(fair_side - ask, 4)
        score = round(
            direction_sign
            * (
                confirmed_ret * 100.0
                + slot_ret_live * 65.0
                + ofi_60s * 12.0
                + ofi_30s * 8.0
                + book_imbalance * 7.0
                + max(0.0, direction_sign * live_extension_pct) * 35.0
            ),
            4,
        )
        strength_score = round(confidence + max(0.0, edge) * 100.0 + momentum_abs * 100.0, 4)

        order_amount = self.config.order_size_usdc
        fee_rate = market.fee_rate
        if ask > 0 and fee_rate >= 0:
            net_shares = order_amount * (1.0 - fee_rate * max(0.0, 1.0 - ask)) / ask
            exit_notional = net_shares * fair_side
            exit_fee = net_shares * fee_rate * fair_side * max(0.0, 1.0 - fair_side)
            fee_adj_pnl = round(exit_notional - exit_fee - order_amount, 6)
        else:
            fee_adj_pnl = -1.0

        entry_allowed = True
        reason = "ok"
        if market.closed or not market.accepting_orders:
            entry_allowed = False
            reason = "market_closed"
        elif ask <= 0:
            entry_allowed = False
            reason = "ask_price_zero"
        elif seconds_into_slot < self.config.min_entry_seconds:
            entry_allowed = False
            reason = "waiting_peak_window"
        elif seconds_into_slot > self.config.max_entry_seconds:
            entry_allowed = False
            reason = "entry_window_passed"
        elif seconds_to_close < self.config.min_seconds_left_to_trade:
            entry_allowed = False
            reason = "too_close_to_resolution"
        elif momentum_abs < min_move:
            entry_allowed = False
            reason = "waiting_intraslot_momentum" if confirmation_stage < 2.0 else "intraslot_momentum_too_small"
        elif ask >= float(getattr(self.config, "signal_repriced_ask_cap", 0.65)):
            entry_allowed = False
            reason = "market_already_repriced"
        elif ask > signal_max_entry_price:
            entry_allowed = False
            reason = "market_too_expensive"
        elif confidence < self.config.min_confidence:
            entry_allowed = False
            reason = "confidence_too_low"
        elif self.config.signal_min_alignment_score > 0 and alignment_score < self.config.signal_min_alignment_score:
            entry_allowed = False
            reason = "signal_alignment_weak"
        elif self.config.signal_min_efficiency_ratio > 0 and efficiency_ratio < self.config.signal_min_efficiency_ratio and raw_confidence < 70.0:
            entry_allowed = False
            reason = "choppy_market"
        elif (
            self.config.signal_late_min_alignment_score > 0
            and seconds_into_slot >= int(self.config.signal_late_reversal_seconds)
            and alignment_score < self.config.signal_late_min_alignment_score
            and (reversal_score >= 0.45 or counter_signal_score >= 0.35)
        ):
            entry_allowed = False
            reason = "late_reversal_risk"
        elif edge < self.config.min_edge:
            entry_allowed = False
            reason = "edge_too_small"
        elif fee_adj_pnl <= 0:
            entry_allowed = False
            reason = "fee_adj_pnl_negative"

        return PredictionSnapshot(
            slug=market.slug,
            direction=direction,
            confidence=round(confidence, 2),
            fair_up_price=round(fair_up, 4),
            fair_down_price=round(fair_down, 4),
            market_price=round(chosen_market.price, 4),
            market_bid=round(chosen_market.bid, 4),
            market_ask=round(chosen_market.ask, 4),
            expected_edge=edge,
            strength_score=strength_score,
            score=score,
            seconds_into_slot=seconds_into_slot,
            seconds_to_close=seconds_to_close,
            entry_allowed=entry_allowed,
            reason=reason,
            created_at=utc_now_iso(),
            btc_price=round(current_price, 2),
            features={
                "ret_1m_pct": round(slot_ret_1m if slot_1m_close_price > 0 else slot_ret_live, 4),
                "ret_3m_pct": round(slot_ret_live, 4),
                "ret_5m_pct": 0.0,
                "ret_15m_pct": 0.0,
                "ema_gap_pct": round(ema_gap, 4),
                "vwap_gap_pct": round(vwap_gap, 4),
                "range_position": round(range_position, 4),
                "volume_spike": round(volume_spike, 4),
                "slot_open_price": round(slot_open_price, 4),
                "slot_1m_close_price": round(slot_1m_close_price, 4),
                "slot_2m_close_price": round(slot_2m_close_price, 4),
                "slot_ret_live_pct": round(slot_ret_live, 4),
                "slot_ret_1m_pct": round(slot_ret_1m, 4),
                "slot_ret_2m_pct": round(slot_ret_2m, 4),
                "confirmed_ret_pct": round(confirmed_ret, 4),
                "confirmation_stage": confirmation_stage,
                "live_extension_pct": round(live_extension_pct, 4),
                "min_intraslot_move_pct": round(min_move, 4),
                "strong_intraslot_move_pct": round(strong_move, 4),
                "ofi_30s": round(ofi_30s, 4),
                "ofi_60s": round(ofi_60s, 4),
                "flow_quality": round(flow_quality, 4),
                "book_imbalance": round(book_imbalance, 4),
                "efficiency_ratio": round(efficiency_ratio, 4),
                "realized_vol_8m": round(realized_vol_8m, 4),
                "alignment_score": round(alignment_score, 4),
                "counter_signal_score": round(counter_signal_score, 4),
                "reversal_score": round(reversal_score, 4),
                "raw_confidence": round(raw_confidence, 4),
                "confidence_penalty": round(confidence_penalty, 4),
                "fee_adj_pnl": fee_adj_pnl,
                "fair_side_price": round(fair_side, 4),
                "pricing_efficiency": round(pricing_efficiency, 4),
                "signal_max_entry_price": round(signal_max_entry_price, 4),
                "signal_price_headroom": round(signal_max_entry_price - ask, 4),
            },
        )


class PolymarketLiveClient:
    def __init__(self, config: BotConfig, private_key: str, funder_address: str = "", signature_type: int = 0) -> None:
        self.config = config
        self.private_key = str(private_key or "").strip()
        self.signature_type = max(0, int(signature_type or 0))
        self.wallet_address = Account.from_key(self.private_key).address if self.private_key else ""
        self.profile_address = str(funder_address or self.wallet_address).strip()
        self._client: Any | None = None

    @property
    def configured(self) -> bool:
        return bool(self.private_key and self.profile_address)

    @staticmethod
    def _api_creds(client: Any) -> Any:
        try:
            return client.derive_api_key()
        except Exception:
            return client.create_or_derive_api_key()

    def client(self) -> Any:
        if not self.configured:
            raise RuntimeError("Polymarket private key gerekli")
        if ClobClient is None:
            raise RuntimeError(f"py-clob-client-v2 gerekli: {CLOB_V2_IMPORT_ERROR}")
        if self._client is None:
            client = ClobClient(
                self.config.clob_base_url,
                key=self.private_key,
                chain_id=137,
                signature_type=self.signature_type,
                funder=self.profile_address,
                use_server_time=True,
                retry_on_error=True,
            )
            client.set_api_creds(self._api_creds(client))
            self._client = client
        return self._client

    def collateral_status(self) -> dict[str, Any]:
        client = self.client()
        return client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=self.signature_type,
            )
        )

    @staticmethod
    def _response_dict(response: Any) -> dict[str, Any]:
        if isinstance(response, dict):
            return response
        if hasattr(response, "__dict__"):
            return dict(response.__dict__)
        return {"success": False, "errorMsg": str(response)}

    def buy(self, side: MarketSide, amount_usdc: float, limit_price: float) -> dict[str, Any]:
        client = self.client()
        order = client.create_market_order(
            MarketOrderArgs(
                token_id=side.token_id,
                amount=round(amount_usdc, 6),
                side=BUY,
                price=round(limit_price, 4),
                order_type=OrderType.FOK,
            )
        )
        return self._response_dict(client.post_order(order, OrderType.FOK))

    def sell(self, side: MarketSide, shares: float, limit_price: float) -> dict[str, Any]:
        client = self.client()
        order = client.create_market_order(
            MarketOrderArgs(
                token_id=side.token_id,
                amount=round(shares, 6),
                side=SELL,
                price=round(limit_price, 4),
                order_type=OrderType.FOK,
            )
        )
        return self._response_dict(client.post_order(order, OrderType.FOK))

    def get_order(self, order_id: str) -> dict[str, Any]:
        client = self.client()
        return client.get_order(str(order_id or "").strip())
