"""
Polymarket BTC 5m Up/Down — Lag Arbitraj Backtest
Hipotez: Binance momentum, 5m slot sonucunu tahmin edebilir mi?
"""
from __future__ import annotations
import json, time, urllib.request, sys, math
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DAYS_BACK      = 30          # kaç günlük veri
SLOT_SECONDS   = 300         # 5 dakika
# Trading saatleri: 13:00–19:00 UTC (~NY öğleden sonra)
SLOT_START_UTC = 13 * 3600
SLOT_END_UTC   = 19 * 3600
# ─────────────────────────────────────────────────────────────────────────────


def fetch_binance_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    """Binance 1m kline'larını sayfalı çek."""
    klines = []
    cursor = start_ms
    while cursor < end_ms:
        url = (
            f"https://api.binance.com/api/v3/klines"
            f"?symbol={symbol}&interval={interval}"
            f"&startTime={cursor}&endTime={end_ms}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                batch = json.loads(r.read())
        except Exception as e:
            print(f"  Binance hata: {e}, 3s bekle...")
            time.sleep(3)
            continue
        if not batch:
            break
        klines.extend(batch)
        cursor = batch[-1][0] + 60_000   # sonraki dakikadan devam
        if len(batch) < 1000:
            break
        time.sleep(0.12)   # rate limit
    return klines


def klines_to_dict(klines: list) -> dict[int, dict]:
    """Unix saniye → candle dict."""
    result = {}
    for k in klines:
        ts_sec = k[0] // 1000
        result[ts_sec] = {
            "open":   float(k[1]),
            "high":   float(k[2]),
            "low":    float(k[3]),
            "close":  float(k[4]),
            "volume": float(k[5]),
            "buy_vol": float(k[9]),
        }
    return result


def slot_floor(ts: int) -> int:
    """Verilen Unix ts'i 5m slot başına indir."""
    return ts - (ts % SLOT_SECONDS)


def analyze_slot(slot_ts: int, candles: dict) -> dict | None:
    """
    Bir 5m slot için tüm feature'ları ve sonucu hesapla.
    slot_ts: slot başlangıcı (Unix saniye, 300'ün katı)
    """
    # Slot başlangıç ve bitiş fiyatları
    open_c  = candles.get(slot_ts)
    close_c = candles.get(slot_ts + 240)   # son dakika (4. dk)

    if not open_c or not close_c:
        return None

    p_open  = open_c["open"]
    p_close = close_c["close"]
    if p_open <= 0:
        return None

    outcome = "UP" if p_close > p_open else "DOWN"

    # ── Feature 1: İlk 1 dakika getirisi (slot_ts başlangıcından 1dk sonra) ──
    c1m = candles.get(slot_ts + 60)
    ret_1m = (c1m["close"] - p_open) / p_open * 100 if c1m else 0.0

    # ── Feature 2: İlk 2 dakika getirisi ─────────────────────────────────────
    c2m = candles.get(slot_ts + 120)
    ret_2m = (c2m["close"] - p_open) / p_open * 100 if c2m else 0.0

    # ── Feature 3: Slot öncesi momentum (slot'tan 5dk önce) ──────────────────
    pre_c = candles.get(slot_ts - 60)   # slot başlamadan 1dk önceki kapanış
    pre5_c = candles.get(slot_ts - 300) # 5dk önceki kapanış
    ret_pre1m  = (p_open - pre_c["close"]) / pre_c["close"] * 100  if pre_c  else 0.0
    ret_pre5m  = (p_open - pre5_c["close"]) / pre5_c["close"] * 100 if pre5_c else 0.0

    # ── Feature 4: OFI yaklaşımı (alış vol / toplam vol) ─────────────────────
    total_vol = sum(
        candles[slot_ts + i * 60]["volume"]
        for i in range(2)
        if candles.get(slot_ts + i * 60)
    )
    buy_vol = sum(
        candles[slot_ts + i * 60]["buy_vol"]
        for i in range(2)
        if candles.get(slot_ts + i * 60)
    )
    ofi = (buy_vol / total_vol - 0.5) * 2 if total_vol > 0 else 0.0  # -1..+1

    # ── Feature 5: Volatilite (önceki 15 dakika) ─────────────────────────────
    prev_closes = [
        candles[slot_ts - i * 60]["close"]
        for i in range(1, 16)
        if candles.get(slot_ts - i * 60)
    ]
    if len(prev_closes) >= 5:
        mean = sum(prev_closes) / len(prev_closes)
        vol  = math.sqrt(sum((x - mean) ** 2 for x in prev_closes) / len(prev_closes)) / mean * 100
    else:
        vol = 999.0

    # ── Feature 6: Slot öncesi trend (15dk) ──────────────────────────────────
    c15m = candles.get(slot_ts - 900)
    trend_15m = (p_open - c15m["close"]) / c15m["close"] * 100 if c15m else 0.0

    return {
        "slot_ts":    slot_ts,
        "outcome":    outcome,
        "p_open":     round(p_open, 2),
        "p_close":    round(p_close, 2),
        "ret_1m":     round(ret_1m, 4),
        "ret_2m":     round(ret_2m, 4),
        "ret_pre1m":  round(ret_pre1m, 4),
        "ret_pre5m":  round(ret_pre5m, 4),
        "ofi":        round(ofi, 4),
        "vol_15m":    round(vol, 4),
        "trend_15m":  round(trend_15m, 4),
    }


def test_strategy(slots: list[dict], name: str, predict_fn) -> dict:
    """Strateji uygula, istatistik döndür."""
    results = []
    skipped = 0
    for s in slots:
        pred = predict_fn(s)
        if pred is None:
            skipped += 1
            continue
        correct = (pred == s["outcome"])
        results.append(correct)
    if not results:
        return {"name": name, "trades": 0, "win_rate": 0, "skipped": skipped}
    win_rate = sum(results) / len(results) * 100
    return {
        "name":      name,
        "trades":    len(results),
        "wins":      sum(results),
        "win_rate":  round(win_rate, 2),
        "skipped":   skipped,
    }


def main():
    now = int(time.time())
    start = now - DAYS_BACK * 86400
    # 1 dakika öncesine kadar al (son mum tamamlansın)
    end = now - 60

    print(f"Binance 1m kline çekiliyor: {DAYS_BACK} gün ({DAYS_BACK*24*60} mum bekleniyor)...")
    klines = fetch_binance_klines("BTCUSDT", "1m", start * 1000, end * 1000)
    print(f"  {len(klines)} mum çekildi.")

    candles = klines_to_dict(klines)

    # Tüm 5m slotlarını üret (yalnızca trading saatleri)
    all_slots = []
    cursor = slot_floor(start)
    while cursor <= end - SLOT_SECONDS:
        ts_of_day = cursor % 86400
        if SLOT_START_UTC <= ts_of_day < SLOT_END_UTC:
            s = analyze_slot(cursor, candles)
            if s:
                all_slots.append(s)
        cursor += SLOT_SECONDS

    print(f"  Analiz edilebilir slot: {len(all_slots)}")
    if not all_slots:
        print("Yeterli veri yok.")
        return

    # Sonuç dağılımı
    n_up   = sum(1 for s in all_slots if s["outcome"] == "UP")
    n_down = sum(1 for s in all_slots if s["outcome"] == "DOWN")
    print(f"  UP: {n_up} ({n_up/len(all_slots)*100:.1f}%)  DOWN: {n_down} ({n_down/len(all_slots)*100:.1f}%)")
    print()

    # ── Stratejiler ───────────────────────────────────────────────────────────
    strategies = [
        # Baseline: rastgele / her zaman UP
        ("Baseline (hep UP)",
         lambda s: "UP"),

        # A: 1dk momentum
        ("A: 1dk momentum (>0 → UP)",
         lambda s: "UP" if s["ret_1m"] > 0 else "DOWN"),

        # B: 1dk momentum, sadece güçlü hareket
        ("B: 1dk momentum filtreli (|ret|>0.03%)",
         lambda s: ("UP" if s["ret_1m"] > 0.03 else "DOWN" if s["ret_1m"] < -0.03 else None)),

        # C: 2dk momentum
        ("C: 2dk momentum (>0 → UP)",
         lambda s: "UP" if s["ret_2m"] > 0 else "DOWN"),

        # D: Pre-slot 1dk momentum (slot başlamadan önce)
        ("D: Slot öncesi 1dk momentum",
         lambda s: "UP" if s["ret_pre1m"] > 0 else "DOWN"),

        # E: Pre-slot 5dk momentum
        ("E: Slot öncesi 5dk momentum",
         lambda s: "UP" if s["ret_pre5m"] > 0 else "DOWN"),

        # F: OFI (alış baskısı > satış → UP)
        ("F: OFI >0 → UP",
         lambda s: "UP" if s["ofi"] > 0 else "DOWN"),

        # G: OFI filtreli
        ("G: OFI filtreli (|ofi|>0.15)",
         lambda s: ("UP" if s["ofi"] > 0.15 else "DOWN" if s["ofi"] < -0.15 else None)),

        # H: 15dk trend devam
        ("H: 15dk trend devamı",
         lambda s: "UP" if s["trend_15m"] > 0 else "DOWN"),

        # I: Kontrarian (1dk momentum tersine yatır)
        ("I: Kontrarian 1dk (tersi)",
         lambda s: "DOWN" if s["ret_1m"] > 0 else "UP"),

        # J: Kombine: OFI + pre-slot momentum aynı yönde
        ("J: Kombine OFI+pre1m aynı yön",
         lambda s: (
             "UP"   if s["ofi"] > 0.05 and s["ret_pre1m"] > 0 else
             "DOWN" if s["ofi"] < -0.05 and s["ret_pre1m"] < 0 else None
         )),

        # K: Düşük volatilite + momentum
        ("K: Düşük vol + 2dk momentum",
         lambda s: (
             ("UP" if s["ret_2m"] > 0 else "DOWN") if s["vol_15m"] < 0.08 else None
         )),

        # L: Yüksek volatilite kontrarian
        ("L: Yüksek vol + kontrarian 1dk",
         lambda s: (
             ("DOWN" if s["ret_1m"] > 0 else "UP") if s["vol_15m"] > 0.12 else None
         )),
    ]

    print("=" * 72)
    print(f"{'Strateji':<45} {'Trade':>6} {'Win':>5} {'WinRate':>8} {'Skipped':>8}")
    print("=" * 72)
    best = None
    for name, fn in strategies:
        r = test_strategy(all_slots, name, fn)
        flag = ""
        if r["trades"] > 50 and r["win_rate"] > 54:
            flag = " ◄◄"
        if best is None or (r["trades"] > 50 and r["win_rate"] > best.get("win_rate", 0)):
            best = r
        print(f"{name:<45} {r['trades']:>6} {r.get('wins',0):>5} {r['win_rate']:>7.1f}%{flag:>4}  skip={r['skipped']}")

    print("=" * 72)
    print()

    # ── Entry price filtresi etkisi ───────────────────────────────────────────
    print("Entry fiyat etkisi (piyasa UP tokenı kaç'tan fiyatlandırıyor):")
    print("(Bu kısmı simüle etmek için Polymarket bid/ask gerekir — atlanıyor)")
    print()

    # ── En iyi strateji için detay ────────────────────────────────────────────
    if best and best["trades"] > 0:
        print(f"En iyi strateji: {best['name']}")
        print(f"  {best['trades']} trade, %{best['win_rate']} win rate")
        print()
        if best["win_rate"] > 54:
            print("YORUM: Anlamlı bir edge var gibi görünüyor.")
            print("Ancak gerçek Polymarket spread+fee'si ~%2 olduğundan,")
            print("%55+ win rate gerekli — 50/50 binary market için.")
        else:
            print("YORUM: Anlamlı edge bulunamadı.")
            print("Binance momentum tek başına 5m Polymarket sonucunu tahmin etmiyor.")

    # Sonuçları JSON olarak kaydet
    with open("C:/Users/Burakcan/Desktop/yedekpolycoin/backtest_results.json", "w", encoding="utf-8") as f:
        json.dump({
            "total_slots": len(all_slots),
            "up_pct": round(n_up / len(all_slots) * 100, 2),
            "down_pct": round(n_down / len(all_slots) * 100, 2),
            "strategies": [test_strategy(all_slots, n, fn) for n, fn in strategies],
        }, f, ensure_ascii=False, indent=2)
    print("Sonuçlar backtest_results.json dosyasına kaydedildi.")


if __name__ == "__main__":
    main()
