# Polymarket BTC 5m Bot

Bu proje artık sadece Polymarket üzerindeki `Bitcoin Up or Down - 5 Dakika` marketini izleyen, BTC yön tahmini üreten ve buna göre `paper` veya `live` modda işlem açan bir bottur.

## Kurulum

```bash
pip install -r requirements.txt
```

## Çalıştır

```bash
python app.py
```

Panel varsayılan olarak `http://127.0.0.1:8765` adresinde açılır.

## Tek cycle

```bash
python app.py --run-once
```

Varsayılan state dosyası artık `runtime/polymarket_btc_5m_state.json` olarak ayrıdır; eski futures state kayıtları bu akışa karışmaz.

## Çalışma mantığı

- Bot yalnızca aktif BTC `Up or Down - 5m` slotunu bulur.
- Tahmin motoru, Binance `BTCUSDT` 1 dakikalık verisinden kısa vadeli momentum sinyali üretir.
- Bu sinyal Polymarket fiyatlarıyla karşılaştırılır.
- Bot önce slotu izler, slot içindeki en güçlü sinyali takip eder ve erken giriş yerine tanımlı pencere içinde peak sinyalde ya da peak'e çok yakın güçteyken `UP` veya `DOWN` tarafında tek pozisyon açar.
- Pozisyon karda/zararda erken kapatılabilir ya da market çözülmesini bekler; zaman doluyor diye ayrıca `time exit` uygulanmaz.
- Varsayılan kurulum güvenlik için `paper` modundadır. Canlıya geçmek için ayarlardan `live` hedeflenir; istenirse önce `paper_grace_minutes` kadar paper davranıp süre bitince live açmayı dener.

## Modlar

- `paper`: Tamamen simülasyon.
- `live`: Hedef moddur. İstersen grace süresi verip botun önce paper davranmasını sağlayabilirsin.

Paper modu 5.5 "classic" profilde çalışır: gerçek BTC/Polymarket verisiyle confidence,
edge, fiyat ve timing filtreleri kullanılır; ekstra alignment/efficiency/reversal
filtreleri varsayılan olarak trade kesmez.

Pozisyon boyutu live/paper aynıdır:
`order_size_usdc=10` baz lot kullanılır. Classic profilde high-confidence büyütme
kapalıdır (`high_confidence_threshold=99`, `high_confidence_order_size_usdc=10`).
Giriş fiyatı `max_entry_price=0.51` ile sınırlandırılır; paper simülasyon canlı limit
emir davranışını taklit etmek için `ask + market_order_buffer` kullanır ama `0.51`
üstüne çıkmaz.
Trade açıldıktan sonra pozisyon `hold_until_resolution=True` gereği marketin kapanmasına kadar tutulur;
kâr/zarar hedefleri (`take_profit_delta`, `stop_loss_delta`) ve canlı modda `live_emergency_exit_bid` koruması devrededir.

## SDK

Canlı emir katmanı Polymarket CLOB V2 SDK kullanır:

```bash
pip install py-clob-client-v2==1.0.0
```

V2 geçişiyle canlı collateral `pUSD` tarafındadır. Botun canlı emir açabilmesi için cüzdanda yeterli collateral bakiyesi ve allowance olmalıdır.

## Secrets

Secrets varsayılan olarak repo kökündeki `.env` dosyasından okunur. Örnek:

```env
TELEGRAM_BOT_TOKEN=""
TELEGRAM_CHAT_ID=""
TELEGRAM_API_BASE_URL="https://api.telegram.org"
POLYMARKET_PRIVATE_KEY=""
POLYMARKET_FUNDER_ADDRESS=""
POLYMARKET_SIGNATURE_TYPE="0"
```

Paneldeki `Secrets Kaydet` butonu da varsayılan kurulumda `.env` dosyasını günceller.
Eski JSON dosyasını kullanmak istersen uygulamayı `--secrets-file runtime/secrets.json`
ile başlatabilirsin.

Ortam değişkenleri de desteklenir ve `.env` değerlerinin üzerine yazılır:

```powershell
$env:TELEGRAM_BOT_TOKEN="..."
$env:TELEGRAM_CHAT_ID="..."
$env:POLYMARKET_PRIVATE_KEY="..."
$env:POLYMARKET_FUNDER_ADDRESS="..."
$env:POLYMARKET_SIGNATURE_TYPE="0"
python app.py
```

## Notlar

- Polymarket crypto marketlerinde taker fee vardır; paper mod bu ücreti yaklaşık olarak modeller.
- Telegram secrets tanımlıysa bot trade bildirimlerine ek olarak saatlik kısa durum raporu da gönderir.
- `live` mod için cüzdanda pUSD/collateral bakiyesi ve gerekli allowance ayarları hazır olmalıdır.
- Eğer live pozisyon kapanmadan resolve olursa kazanan payların ayrıca redeem edilmesi gerekebilir.
