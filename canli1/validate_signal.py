from __future__ import annotations

import time
from datetime import datetime, timezone

from spotbot.models import BotConfig
from spotbot.polymarket import BinanceBtcClient, PolymarketPublicClient, SignalEngine
from spotbot.ws_feeds import BinanceWsFeeder, PolymarketWsFeeder


UTC = timezone.utc


def _agree(a: str, b: str) -> str:
    if not a or not b:
        return "?"
    return "✅ EŞLEŞİYOR" if a == b else "❌ ÇELİŞİYOR"


def validate_once(rounds: int = 5, wait_sec: float = 30.0) -> None:
    """Sinyal üret → gerçek piyasa ile eşle."""
    cfg = BotConfig()
    poly = PolymarketPublicClient(cfg)
    btc = BinanceBtcClient(cfg.price_source_base_url, timeout=cfg.request_timeout_sec)
    engine = SignalEngine(cfg)
    binance_ws = BinanceWsFeeder()
    poly_ws = PolymarketWsFeeder()
    binance_ws.start()
    poly_ws.start()
    print("WebSocket feed'ler başlatılıyor, 12s bekliyor...")
    time.sleep(12)

    results: list[dict] = []

    for i in range(rounds):
        print(f"\n{'=' * 70}")
        print(f"TUR {i + 1}/{rounds} | {datetime.now().strftime('%H:%M:%S')}")
        print('=' * 70)

        market = poly.get_current_market()
        if not market:
            print("❌ Market bulunamadı, atla")
            time.sleep(wait_sec)
            continue

        poly_ws.resubscribe([market.up.token_id, market.down.token_id])
        time.sleep(2)

        ctx = binance_ws.get_context(cfg.signal_lookback_minutes) if binance_ws.is_ready() else btc.fetch_context(cfg.signal_lookback_minutes)
        sig = engine.build_signal(market, ctx)

        closes = [c["close"] for c in ctx["candles"] if c.get("close", 0) > 0]
        price_now = ctx["price"]
        price_5m_ago = closes[-6] if len(closes) >= 6 else closes[0]
        actual_momentum_5m = "UP" if price_now > price_5m_ago else "DOWN"

        market_leader = "UP" if market.up.price >= market.down.price else "DOWN"

        match_market = _agree(sig.direction, market_leader)
        match_momentum = _agree(sig.direction, actual_momentum_5m)

        print(f"Slot: {market.slug}")
        print(f"BTC fiyat   : ${price_now:.2f}  (5dk önce: ${price_5m_ago:.2f})")
        print(f"5dk momentum: {actual_momentum_5m}  ({((price_now / price_5m_ago - 1) * 100):+.3f}%)")
        print()
        print(f"Bot sinyali : {sig.direction}  conf={sig.confidence:.1f}  edge={sig.expected_edge:+.4f}  score={sig.score:+.2f}")
        print(f"Piyasa lider: {market_leader}  (UP={market.up.price:.3f} / DOWN={market.down.price:.3f})")
        print(f"Ask         : {sig.market_ask:.3f}")
        print(f"fee_adj_pnl : {sig.features.get('fee_adj_pnl', 0):.4f}")
        print(f"Entry_allowed: {sig.entry_allowed}  reason={sig.reason}")
        print()
        print(f"Sinyal ↔ Piyasa lideri : {match_market}")
        print(f"Sinyal ↔ 5dk momentum  : {match_momentum}")

        results.append({
            "signal": sig.direction,
            "market_leader": market_leader,
            "momentum_5m": actual_momentum_5m,
            "match_market": sig.direction == market_leader,
            "match_momentum": sig.direction == actual_momentum_5m,
            "entry_allowed": sig.entry_allowed,
            "reason": sig.reason,
        })

        if i < rounds - 1:
            print(f"\n{wait_sec:.0f}s bekleniyor...")
            time.sleep(wait_sec)

    print(f"\n{'=' * 70}")
    print("ÖZET")
    print('=' * 70)
    n = len(results)
    if n == 0:
        print("Hiç veri yok")
        return
    match_mkt = sum(1 for r in results if r["match_market"])
    match_mom = sum(1 for r in results if r["match_momentum"])
    allowed = sum(1 for r in results if r["entry_allowed"])
    print(f"Toplam tur        : {n}")
    print(f"Piyasa eşleşmesi  : {match_mkt}/{n} ({match_mkt / n * 100:.0f}%)")
    print(f"Momentum eşleşme  : {match_mom}/{n} ({match_mom / n * 100:.0f}%)")
    print(f"Entry allowed     : {allowed}/{n}")
    print(f"Reason dağılımı   :")
    from collections import Counter
    for reason, count in Counter(r["reason"] for r in results).most_common():
        print(f"   {reason}: {count}")


if __name__ == "__main__":
    validate_once(rounds=5, wait_sec=30.0)
