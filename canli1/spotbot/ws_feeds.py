from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from typing import Any

import websocket

from .utils import safe_float

logger = logging.getLogger(__name__)


class BinanceWsFeeder:
    """
    Background WebSocket feed for Binance BTCUSDT.
    Streams kline_1m (completed candles + live candle) and aggTrade (tick-by-tick).

    OFI (order flow imbalance) is pre-computed in 1-second buckets so get_context()
    runs in O(120) not O(N_trades). Binance BTC/USDT fires ~30 aggTrades/sec so the
    old approach was iterating ~1800 items on every 8-second poll cycle.
    """

    _WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1m/btcusdt@aggTrade/btcusdt@depth5@100ms"
    _MAX_CANDLES = 120
    _BUCKET_SECONDS = 120  # keep 2 minutes of 1-second buckets

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._candles: list[dict[str, float]] = []
        self._live_candle: dict[str, float] = {}
        self._current_price: float = 0.0

        # 1-second aggregation buckets: deque of [ts_floor_sec, buy_vol, sell_vol]
        # is_buyer_maker=True means the aggressor was the seller → sell volume
        self._buckets: deque[list[float]] = deque(maxlen=self._BUCKET_SECONDS)
        self._cur_bucket: list[float] = [0.0, 0.0, 0.0]  # [ts_sec, buy_vol, sell_vol]
        self._book_imbalance: float = 0.0
        self._book_bid_qty: float = 0.0
        self._book_ask_qty: float = 0.0

        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._reconnect_delay = 1.0
        self._last_receive_ts = 0.0
        self._last_event_ts = 0.0
        self._stop_flag = threading.Event()

    def start(self) -> None:
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="binance-ws")
        self._thread.start()

    def stop(self) -> None:
        self._stop_flag.set()
        with self._lock:
            ws = self._ws
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def is_ready(self) -> bool:
        with self._lock:
            return len(self._candles) >= 20 and self._current_price > 0

    def status(self) -> dict[str, float | bool]:
        with self._lock:
            now = time.time()
            last_receive_ts = self._last_receive_ts
            last_event_ts = self._last_event_ts
            price = self._current_price
            ready = len(self._candles) >= 20 and price > 0
        receive_age = now - last_receive_ts if last_receive_ts > 0 else 999999.0
        event_lag = now - last_event_ts if last_event_ts > 0 else 999999.0
        return {
            "ready": ready,
            "last_receive_age_seconds": round(receive_age, 3),
            "last_event_lag_seconds": round(event_lag, 3),
            "price": price,
        }

    def is_fresh(self, max_age_seconds: float) -> bool:
        status = self.status()
        return bool(
            safe_float(status["price"]) > 0
            and safe_float(status["last_receive_age_seconds"], 999999.0) <= max_age_seconds
        )

    def get_context(self, lookback_minutes: int = 30) -> dict[str, Any]:
        with self._lock:
            candles = list(self._candles[-lookback_minutes:])
            if self._live_candle:
                candles = candles + [dict(self._live_candle)]
            price = self._current_price
            # snapshot: completed buckets + current partial bucket
            buckets = list(self._buckets) + [list(self._cur_bucket)]
            book_imbalance = self._book_imbalance
            book_bid_qty = self._book_bid_qty
            book_ask_qty = self._book_ask_qty

        # O(120) — one pass over 1-second buckets, not O(N_trades)
        now_sec = time.time()
        buy_30 = sell_30 = buy_60 = sell_60 = 0.0
        for b in buckets:
            age = now_sec - b[0]
            if age <= 60:
                buy_60 += b[1]
                sell_60 += b[2]
                if age <= 30:
                    buy_30 += b[1]
                    sell_30 += b[2]

        total_30 = buy_30 + sell_30 or 1.0
        total_60 = buy_60 + sell_60 or 1.0

        return {
            "price": price,
            "candles": candles,
            "source": "Binance WS BTCUSDT 1m+aggTrade",
            "trade_momentum": {
                "ofi_30s": (buy_30 - sell_30) / total_30,
                "ofi_60s": (buy_60 - sell_60) / total_60,
                "buy_vol_30s": buy_30,
                "sell_vol_30s": sell_30,
                "buy_vol_60s": buy_60,
                "sell_vol_60s": sell_60,
                "book_imbalance": book_imbalance,
                "book_bid_qty": book_bid_qty,
                "book_ask_qty": book_ask_qty,
            },
        }

    def _handle_kline(self, data: dict[str, Any]) -> None:
        k = data.get("k", {})
        candle = {
            "open_time": safe_float(k.get("t")),
            "open": safe_float(k.get("o")),
            "high": safe_float(k.get("h")),
            "low": safe_float(k.get("l")),
            "close": safe_float(k.get("c")),
            "volume": safe_float(k.get("v")),
        }
        with self._lock:
            self._current_price = candle["close"]
            if k.get("x"):  # candle closed — append to history
                self._candles.append(candle)
                if len(self._candles) > self._MAX_CANDLES:
                    self._candles = self._candles[-self._MAX_CANDLES:]
                self._live_candle = {}
            else:
                self._live_candle = candle

    def _handle_agg_trade(self, data: dict[str, Any]) -> None:
        ts_ms = safe_float(data.get("T"))
        price = safe_float(data.get("p"))
        qty = safe_float(data.get("q"))
        is_buyer_maker = bool(data.get("m", False))
        if price <= 0 or qty <= 0:
            return
        ts_sec = ts_ms / 1000.0  # keep float precision
        with self._lock:
            self._current_price = price
            # Flush completed bucket when second changes
            if ts_sec - self._cur_bucket[0] >= 1.0:
                if self._cur_bucket[1] + self._cur_bucket[2] > 0:
                    self._buckets.append(list(self._cur_bucket))
                self._cur_bucket = [ts_sec, 0.0, 0.0]
            if is_buyer_maker:
                self._cur_bucket[2] += qty  # sell aggressor
            else:
                self._cur_bucket[1] += qty  # buy aggressor

    def _handle_depth(self, data: dict[str, Any]) -> None:
        bids = data.get("b", data.get("bids", []))
        asks = data.get("a", data.get("asks", []))
        bid_qty = sum(safe_float(row[1]) for row in bids[:5] if isinstance(row, list) and len(row) >= 2)
        ask_qty = sum(safe_float(row[1]) for row in asks[:5] if isinstance(row, list) and len(row) >= 2)
        total = bid_qty + ask_qty
        if total <= 0:
            return
        with self._lock:
            self._book_bid_qty = bid_qty
            self._book_ask_qty = ask_qty
            self._book_imbalance = (bid_qty - ask_qty) / total

    def _on_message(self, ws: Any, raw: str) -> None:
        try:
            msg = json.loads(raw)
            stream = msg.get("stream", "")
            data = msg.get("data", {})
            receive_ts = time.time()
            event_ms = safe_float(data.get("E"), safe_float(data.get("T")))
            with self._lock:
                self._last_receive_ts = receive_ts
                if event_ms > 0:
                    self._last_event_ts = event_ms / 1000.0
            if "kline" in stream:
                self._handle_kline(data)
            elif "aggTrade" in stream:
                self._handle_agg_trade(data)
            elif "depth" in stream:
                self._handle_depth(data)
        except Exception as exc:
            logger.warning("Binance WS message parse error: %s", exc)

    def _on_open(self, ws: Any) -> None:
        with self._lock:
            self._reconnect_delay = 1.0

    def _on_close(self, ws: Any, *args: Any) -> None:
        pass

    def _on_error(self, ws: Any, error: Any) -> None:
        logger.debug("Binance WS error: %s", error)

    def _run_loop(self) -> None:
        while not self._stop_flag.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self._WS_URL,
                    on_message=self._on_message,
                    on_open=self._on_open,
                    on_close=self._on_close,
                    on_error=self._on_error,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                logger.debug("Binance WS run error: %s", exc)
            if self._stop_flag.is_set():
                break
            with self._lock:
                delay = self._reconnect_delay
                self._reconnect_delay = min(self._reconnect_delay * 2, 30.0)
            time.sleep(delay)


class PolymarketWsFeeder:
    """
    Background WebSocket feed for Polymarket CLOB real-time YES/NO prices.
    Call resubscribe(token_ids) whenever the active market changes.
    Call get_price(token_id) to read latest bid/ask/mid.
    """

    _WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._prices: dict[str, dict[str, float]] = {}
        self._pending_ids: list[str] = []
        self._connected = False
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._reconnect_delay = 1.0
        self._last_receive_ts = 0.0
        self._stop_flag = threading.Event()

    def start(self) -> None:
        self._stop_flag.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="polymarket-ws")
        self._thread.start()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True, name="polymarket-ws-heartbeat")
        self._heartbeat_thread.start()

    def stop(self) -> None:
        self._stop_flag.set()
        with self._lock:
            ws = self._ws
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def resubscribe(self, token_ids: list[str]) -> None:
        with self._lock:
            self._pending_ids = list(token_ids)
            connected = self._connected
            ws = self._ws
        if connected and ws and token_ids:
            try:
                ws.send(json.dumps({"assets_ids": token_ids, "type": "Market"}))
            except Exception:
                pass

    def get_price(self, token_id: str) -> dict[str, float] | None:
        with self._lock:
            entry = self._prices.get(token_id)
            return dict(entry) if entry else None

    def status(self, token_ids: list[str] | None = None) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            connected = self._connected
            last_receive_ts = self._last_receive_ts
            prices = {key: dict(value) for key, value in self._prices.items()}
        ids = token_ids or list(prices)
        token_ages: dict[str, float] = {}
        for token_id in ids:
            updated_ts = safe_float(prices.get(token_id, {}).get("updated_ts"))
            token_ages[token_id] = round(now - updated_ts, 3) if updated_ts > 0 else 999999.0
        receive_age = now - last_receive_ts if last_receive_ts > 0 else 999999.0
        return {
            "connected": connected,
            "last_receive_age_seconds": round(receive_age, 3),
            "token_age_seconds": token_ages,
        }

    def is_price_fresh(self, token_id: str, max_age_seconds: float) -> bool:
        entry = self.get_price(token_id)
        if not entry:
            return False
        age = time.time() - safe_float(entry.get("updated_ts"))
        return age <= max_age_seconds and safe_float(entry.get("bid")) > 0 and safe_float(entry.get("ask")) > 0

    def _send_subscribe(self, ws: Any, token_ids: list[str]) -> None:
        if not token_ids:
            return
        try:
            ws.send(json.dumps({"assets_ids": token_ids, "type": "Market"}))
        except Exception:
            pass

    def _on_open(self, ws: Any) -> None:
        with self._lock:
            self._connected = True
            self._reconnect_delay = 1.0
            pending = list(self._pending_ids)
        if pending:
            self._send_subscribe(ws, pending)

    def _on_close(self, ws: Any, *args: Any) -> None:
        with self._lock:
            self._connected = False

    def _on_error(self, ws: Any, error: Any) -> None:
        with self._lock:
            self._connected = False

    def _on_message(self, ws: Any, raw: str) -> None:
        try:
            events = json.loads(raw)
            with self._lock:
                self._last_receive_ts = time.time()
            if not isinstance(events, list):
                events = [events]
            for event in events:
                self._handle_event(event)
        except Exception as exc:
            logger.warning("Polymarket WS message parse error: %s", exc)

    def _handle_event(self, event: dict[str, Any]) -> None:
        etype = event.get("event_type", "")
        asset_id = str(event.get("asset_id", ""))
        if not asset_id:
            return

        with self._lock:
            if asset_id not in self._prices:
                self._prices[asset_id] = {"bid": 0.0, "ask": 0.0, "price": 0.0, "updated_ts": 0.0}
            entry = self._prices[asset_id]
            entry["updated_ts"] = time.time()

            if etype == "price_change":
                for change in event.get("changes", []):
                    side = str(change.get("side", "")).upper()
                    price = safe_float(change.get("price"))
                    if price <= 0:
                        continue
                    if side == "BUY":
                        entry["bid"] = price
                    elif side == "SELL":
                        entry["ask"] = price
                if entry["bid"] > 0 and entry["ask"] > 0:
                    entry["price"] = round((entry["bid"] + entry["ask"]) / 2, 4)

            elif etype == "book":
                bids = event.get("bids", [])
                asks = event.get("asks", [])
                if bids:
                    best_bid = max((safe_float(b.get("price", 0)) for b in bids), default=0.0)
                    if best_bid > 0:
                        entry["bid"] = best_bid
                if asks:
                    best_ask = min(
                        (safe_float(a.get("price", 0)) for a in asks if safe_float(a.get("price", 0)) > 0),
                        default=0.0,
                    )
                    if best_ask > 0:
                        entry["ask"] = best_ask
                if entry["bid"] > 0 and entry["ask"] > 0:
                    entry["price"] = round((entry["bid"] + entry["ask"]) / 2, 4)

            elif etype == "last_trade_price":
                price = safe_float(event.get("price"))
                if price > 0:
                    entry["price"] = price

    def _heartbeat_loop(self) -> None:
        while not self._stop_flag.is_set():
            with self._lock:
                ws = self._ws
                connected = self._connected
            if connected and ws:
                try:
                    ws.send("PING")
                except Exception:
                    pass
            self._stop_flag.wait(timeout=10)

    def _run_loop(self) -> None:
        while not self._stop_flag.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self._WS_URL,
                    on_message=self._on_message,
                    on_open=self._on_open,
                    on_close=self._on_close,
                    on_error=self._on_error,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                logger.debug("Polymarket WS run error: %s", exc)
            if self._stop_flag.is_set():
                break
            with self._lock:
                delay = self._reconnect_delay
                self._reconnect_delay = min(self._reconnect_delay * 2, 30.0)
            time.sleep(delay)
