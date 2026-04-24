const numericFields = [
  "paper_grace_minutes",
  "poll_seconds",
  "signal_lookback_minutes",
  "paper_balance_usdc",
  "order_size_usdc",
  "high_confidence_threshold",
  "high_confidence_order_size_usdc",
  "min_confidence",
  "min_edge",
  "max_entry_price",
  "min_entry_seconds",
  "max_entry_seconds",
  "min_seconds_left_to_trade",
  "take_profit_delta",
  "stop_loss_delta",
  "market_order_buffer",
  "paper_taker_fee_rate",
  "live_entry_price_buffer",
  "live_orderbook_depth_multiplier",
  "live_orderbook_max_age_seconds",
  "binance_ws_max_age_seconds",
  "polymarket_ws_max_age_seconds",
  "polymarket_signature_type",
];

let refreshTimer = null;
let isShuttingDown = false;

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let payload = {};
  try {
    payload = await response.json();
  } catch {
    payload = { ok: false, message: `API cevabı okunamadı (${response.status})` };
  }
  if (!response.ok) {
    throw new Error(payload.message || `HTTP ${response.status}`);
  }
  return payload;
}

function fmtNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString("tr-TR", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value;
  }
}

function setActionMessage(message) {
  if (!message) return;
  setText("secretStatusText", message);
  setText("configStatusText", message);
}

function rowHtml(columns) {
  return `<tr>${columns.map((value) => `<td>${value}</td>`).join("")}</tr>`;
}

function pnlClass(value) {
  if (value > 0) return "positive";
  if (value < 0) return "negative";
  return "neutral";
}

function fillConfig(config) {
  const form = document.getElementById("configForm");
  if (!form) return;
  Object.entries(config || {}).forEach(([key, value]) => {
    const input = form.elements.namedItem(key);
    if (input) {
      input.value = typeof value === "boolean" ? String(value) : value;
    }
  });
}

function fillSecrets(secrets) {
  const telegramToken = document.getElementById("telegramBotToken");
  const telegramChatId = document.getElementById("telegramChatId");
  const telegramApiBaseUrl = document.getElementById("telegramApiBaseUrl");
  const polyPrivateKey = document.getElementById("polyPrivateKey");
  const polyFunderAddress = document.getElementById("polyFunderAddress");
  const polySignatureType = document.getElementById("polySignatureType");
  if (telegramToken) telegramToken.placeholder = secrets.telegram_token_hint || "Kayıtlı değil";
  if (telegramChatId) telegramChatId.placeholder = secrets.telegram_chat_id_hint || "Kayıtlı değil";
  if (telegramApiBaseUrl && !telegramApiBaseUrl.value) telegramApiBaseUrl.value = secrets.telegram_api_base_url || "https://api.telegram.org";
  if (polyPrivateKey) polyPrivateKey.placeholder = secrets.polymarket_private_key_hint || "Kayıtlı değil";
  if (polyFunderAddress) polyFunderAddress.placeholder = secrets.polymarket_funder_address_hint || "Kayıtlı değil";
  if (polySignatureType && !polySignatureType.value) polySignatureType.value = secrets.polymarket_signature_type ?? 0;
  setText("telegramSecretHint", secrets.telegram_configured ? (secrets.telegram_chat_id_hint || "Hazır") : "Eksik");
  setText("polySecretHint", secrets.polymarket_configured ? (secrets.polymarket_funder_address_hint || "Hazır") : "Eksik");
}

function detailRows(data) {
  return Object.entries(data)
    .map(([key, value]) => `<div class="detail-item"><span>${key}</span><strong>${value}</strong></div>`)
    .join("");
}

function renderActiveTrade(trade) {
  const box = document.getElementById("activeTradeBox");
  if (!box) return;
  if (!trade) {
    box.className = "detail-grid empty-box";
    box.innerHTML = "Açık trade yok.";
    setText("activeTradeBadge", "Yok");
    return;
  }
  box.className = "detail-grid";
  box.innerHTML = detailRows({
    Yön: trade.side,
    Durum: trade.status,
    Slot: trade.slot_slug,
    Pay: fmtNumber(trade.shares, 4),
    Entry: fmtNumber(trade.entry_price, 3),
    Notional: `${fmtNumber(trade.entry_notional_usdc, 2)} USDC`,
    "Order ID": trade.entry_order_id || "-",
    "Çıkış Nedeni": trade.exit_reason || "-",
  });
  setText("activeTradeBadge", trade.side);
}

function renderSignal(market, signal) {
  const box = document.getElementById("signalBox");
  if (!box) return;
  if (!market || !signal) {
    box.className = "detail-grid empty-box";
    box.innerHTML = "Sinyal bekleniyor.";
    setText("signalReasonText", "-");
    return;
  }
  box.className = "detail-grid";
  box.innerHTML = detailRows({
    Sinyal: signal.direction,
    Güven: `%${fmtNumber(signal.confidence, 1)}`,
    Edge: fmtNumber(signal.expected_edge, 3),
    "Fair Up": fmtNumber(signal.fair_up_price, 3),
    "Fair Down": fmtNumber(signal.fair_down_price, 3),
    "Market Ask": fmtNumber(signal.market_ask, 3),
    "BTC": fmtNumber(signal.btc_price, 2),
    "İçeride Geçen": `${signal.seconds_into_slot || 0} sn`,
    "Kalan Süre": `${signal.seconds_to_close || 0} sn`,
    "Entry": signal.entry_allowed ? "Açılabilir" : "Pas",
  });
  setText("signalReasonText", signal.reason || "-");
}

function renderBestCycle(analytics) {
  const box = document.getElementById("bestCycleBox");
  if (!box) return;
  const best = analytics?.best_cycle;
  if (!best) {
    box.className = "detail-grid empty-box";
    box.innerHTML = "Henüz yeterli trade verisi yok.";
    setText("bestCycleBadge", "-");
    return;
  }
  box.className = "detail-grid";
  box.innerHTML = detailRows({
    Pencere: best.label,
    "Cycle Aralığı": best.cycle_range,
    "Örneklem": best.sample_count,
    "Win Rate": `%${fmtNumber(best.win_rate_pct, 1)}`,
    "Ort. PnL": `${fmtNumber(best.avg_pnl_usdc, 3)} USDC`,
    "Toplam PnL": `${fmtNumber(best.total_pnl_usdc, 3)} USDC`,
    "Ort. Giriş": `${fmtNumber(best.avg_entry_seconds, 1)} sn`,
  });
  setText("bestCycleBadge", `${best.cycle_range} | ${best.label}`);
}

function renderTrades(trades) {
  const body = document.getElementById("closedTradesBody");
  if (!body) return;
  if (!trades || !trades.length) {
    body.innerHTML = rowHtml(["Henüz trade yok", "", "", "", "", "", ""]);
    return;
  }
  body.innerHTML = trades
    .slice()
    .reverse()
    .map((trade) =>
      rowHtml([
        trade.slot_slug || "-",
        trade.side || "-",
        trade.status || "-",
        fmtNumber(trade.entry_price, 3),
        trade.exit_price ? fmtNumber(trade.exit_price, 3) : "-",
        `<span class="${pnlClass(trade.pnl_usdc)}">${fmtNumber(trade.pnl_usdc, 4)}</span>`,
        trade.exit_reason || trade.resolution_outcome || "-",
      ])
    )
    .join("");
}

function renderLogs(logs) {
  const box = document.getElementById("logsBox");
  if (!box) return;
  box.innerHTML = (logs || [])
    .slice()
    .reverse()
    .map((log) => {
      const extra = log.extra && Object.keys(log.extra).length ? ` | ${JSON.stringify(log.extra)}` : "";
      return `<div class="log-item"><strong>${log.level}</strong> ${log.message}<br><small>${log.ts}${extra}</small></div>`;
    })
    .join("");
}

function renderState(state) {
  const summary = state.summary || {};
  const runtime = state.runtime || {};
  const marketState = state.market || {};
  const market = marketState.current_market;
  const signal = marketState.last_signal;
  const activeTrade = state.portfolio?.active_trade || null;
  const closedTrades = state.portfolio?.closed_trades || [];
  const geo = marketState.geoblock || {};
  const graceRemainingSeconds = Number(summary.paper_grace_remaining_seconds || 0);
  const graceLabel =
    summary.target_mode === "live"
      ? (graceRemainingSeconds > 0 ? `${Math.ceil(graceRemainingSeconds / 60)} dk` : "Bitti")
      : "Kapalı";

  setText("statusText", runtime.running ? "Çalışıyor" : "Durdu");
  setText("modeText", summary.mode || "-");
  setText("targetModeText", summary.target_mode || "-");
  setText("graceText", graceLabel);
  setText("cycleText", runtime.cycle_index ?? "-");
  setText("slotText", market?.slug || "-");
  setText("tradeStatusText", activeTrade ? `${activeTrade.side} / ${activeTrade.status}` : "Yok");
  setText("geoText", geo.blocked ? "Bloklu" : (geo.country ? `${geo.country} açık` : "-"));
  setText("equityText", `${fmtNumber(summary.current_equity_usdc, 2)} USDC`);
  setText("realizedText", `Realized: ${fmtNumber(summary.realized_pnl_usdc, 2)} USDC | ${summary.rollout_status || "-"}`);
  setText("signalText", signal ? signal.direction : "-");
  setText("signalMetaText", signal ? `%${fmtNumber(signal.confidence, 1)} | edge ${fmtNumber(signal.expected_edge, 3)}` : "-");
  setText("marketPriceText", market ? `UP ${fmtNumber(market.up?.ask, 3)} / DOWN ${fmtNumber(market.down?.ask, 3)}` : "-");
  setText("marketMetaText", market ? `${market.question}` : "-");
  setText("winRateText", `%${fmtNumber(summary.win_rate_pct, 1)}`);
  setText("stateFileText", summary.target_mode === "live" ? `Paper ${fmtNumber(summary.paper_equity_usdc, 2)} | Live ${fmtNumber(summary.live_equity_usdc, 2)}` : (runtime.state_file || "-"));

  renderActiveTrade(activeTrade);
  renderSignal(market, signal);
  renderBestCycle(state.analytics || {});
  renderTrades(closedTrades);
  renderLogs(state.logs || []);
  fillConfig(state.config || {});
  fillSecrets(state.secrets || {});
}

async function refreshState() {
  if (isShuttingDown) return;
  try {
    const payload = await api("/api/state");
    if (payload.ok) {
      renderState(payload.state);
    }
  } catch (error) {
    console.error(error);
    setActionMessage(error.message || "State alınamadı");
  }
}

async function postAction(path, body = {}) {
  try {
    const payload = await api(path, {
      method: "POST",
      body: JSON.stringify(body),
    });
    await refreshState();
    setActionMessage(payload.message || "İşlem tamamlandı");
    return payload;
  } catch (error) {
    console.error(error);
    setActionMessage(error.message || "İşlem başarısız");
    return { ok: false, message: error.message || "İşlem başarısız" };
  }
}

document.getElementById("startBtn").addEventListener("click", () => postAction("/api/start"));
document.getElementById("stopBtn").addEventListener("click", () => postAction("/api/stop"));
document.getElementById("runOnceBtn").addEventListener("click", () => postAction("/api/run-once"));
document.getElementById("resetBalanceBtn").addEventListener("click", async () => {
  if (!window.confirm("Paper bakiye ve trade geçmişi sıfırlansın mı?")) return;
  await postAction("/api/reset-balance");
});
document.getElementById("resetPnlBtn").addEventListener("click", async () => {
  if (!window.confirm("Sadece PnL ve trade istatistikleri sıfırlansın mı?")) return;
  await postAction("/api/reset-pnl");
});
document.getElementById("resetBtn").addEventListener("click", async () => {
  if (!window.confirm("Tüm state temizlensin mi?")) return;
  await postAction("/api/reset");
});
document.getElementById("shutdownBtn").addEventListener("click", async () => {
  if (!window.confirm("Yazılım tamamen kapatılsın mı?")) return;
  isShuttingDown = true;
  if (refreshTimer) window.clearInterval(refreshTimer);
  setText("statusText", "Kapatılıyor");
  try {
    await api("/api/shutdown", { method: "POST", body: JSON.stringify({}) });
    setText("statusText", "Kapandı");
  } catch {
    setText("statusText", "Kapandı");
  }
});

document.getElementById("configForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const payload = {};
  for (const [key, value] of formData.entries()) {
    payload[key] = numericFields.includes(key) ? Number(value) : value;
  }
  await postAction("/api/config", payload);
});

document.getElementById("secretsForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const formData = new FormData(event.target);
  const payload = {};
  for (const [key, value] of formData.entries()) {
    if (!String(value || "").trim()) continue;
    payload[key] = numericFields.includes(key) ? Number(value) : value;
  }
  await postAction("/api/secrets", payload);
  event.target.reset();
});

document.getElementById("testTelegramBtn").addEventListener("click", async () => {
  const response = await postAction("/api/test-telegram");
  setActionMessage(response.message || "Telegram test tamamlandı");
});

document.getElementById("testPolymarketBtn").addEventListener("click", async () => {
  const response = await postAction("/api/test-polymarket");
  const marketSlug = response.market?.slug ? ` | ${response.market.slug}` : "";
  setActionMessage((response.message || "Polymarket test tamamlandı") + marketSlug);
});

refreshState();
refreshTimer = setInterval(refreshState, 3000);
