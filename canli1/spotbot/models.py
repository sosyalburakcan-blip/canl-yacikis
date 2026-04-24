from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BotConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    data_base_url: str = "https://data-api.polymarket.com"
    price_source_base_url: str = "https://api.binance.com"
    trading_mode: str = "paper"
    paper_grace_minutes: int = 30
    poll_seconds: int = 8
    resolution_poll_seconds: int = 1
    fast_poll_window_seconds: int = 45
    signal_lookback_minutes: int = 75
    paper_balance_usdc: float = 30.0
    paper_require_live_shadow: bool = True
    order_size_usdc: float = 10.0
    high_confidence_threshold: float = 99.0
    high_confidence_order_size_usdc: float = 10.0
    min_confidence: float = 60.0
    min_edge: float = 0.03
    min_entry_price: float = 0.0
    max_entry_price: float = 0.55
    momentum_max_entry_price: float = 0.63
    min_entry_seconds: int = 60
    max_entry_seconds: int = 180
    min_seconds_left_to_trade: int = 15
    signal_min_intraslot_move_pct: float = 0.03
    signal_strong_intraslot_move_pct: float = 0.05
    signal_repriced_ask_cap: float = 0.65
    signal_min_alignment_score: float = 0.0
    signal_late_min_alignment_score: float = 0.0
    signal_late_reversal_seconds: int = 285
    signal_min_efficiency_ratio: float = 0.0
    signal_min_ofi_volume_btc: float = 1.0
    live_hard_mode: bool = True
    live_min_confidence: float = 60.0
    live_min_edge: float = 0.03
    live_min_fee_adj_pnl_usdc: float = 0.0
    live_min_liquidity_usdc: float = 10000.0
    live_max_spread: float = 0.02
    live_signal_confirmations: int = 1
    live_max_daily_loss_usdc: float = 30.0
    live_max_consecutive_losses: int = 2
    live_max_order_failures: int = 2
    live_max_resolution_delay_seconds: int = 180
    live_position_sync_timeout_seconds: int = 20
    live_require_synced_position: bool = True
    live_clob_v2_guard: bool = True
    live_reprice_before_order: bool = True
    live_entry_price_buffer: float = 0.01
    live_orderbook_depth_multiplier: float = 1.0
    live_orderbook_max_age_seconds: float = 1.5
    binance_ws_max_age_seconds: float = 1.5
    polymarket_ws_max_age_seconds: float = 1.5
    hold_until_resolution: bool = True
    take_profit_delta: float = 0.07
    stop_loss_delta: float = 0.04
    live_emergency_exit_bid: float = 0.12  # bid bu seviyenin altına düşerse canlıda çık
    market_order_buffer: float = 0.01
    paper_taker_fee_rate: float = 0.02
    telegram_trade_notifications: bool = True
    telegram_shadow_market_reports: bool = True
    telegram_hourly_reports: bool = True
    request_timeout_sec: float = 15.0
    state_file: str = "runtime/polymarket_btc_5m_state.json"
    secrets_file: str = ".env"


@dataclass
class MarketSide:
    label: str
    token_id: str
    price: float
    bid: float
    ask: float


@dataclass
class MarketSnapshot:
    slug: str
    question: str
    condition_id: str
    start_time: str
    end_time: str
    active: bool
    closed: bool
    accepting_orders: bool
    restricted: bool
    tick_size: float
    min_order_size: float
    spread: float
    best_bid: float
    best_ask: float
    last_trade_price: float
    volume: float
    liquidity: float
    fees_enabled: bool
    fee_rate: float
    updated_at: str
    up: MarketSide
    down: MarketSide
    resolved_outcome: str = ""

    def side(self, direction: str) -> MarketSide:
        if str(direction).strip().upper() == "DOWN":
            return self.down
        return self.up


@dataclass
class PredictionSnapshot:
    slug: str
    direction: str
    confidence: float
    fair_up_price: float
    fair_down_price: float
    market_price: float
    market_bid: float
    market_ask: float
    expected_edge: float
    strength_score: float
    score: float
    seconds_into_slot: int
    seconds_to_close: int
    entry_allowed: bool
    reason: str
    created_at: str
    btc_price: float = 0.0
    features: dict[str, float] = field(default_factory=dict)


@dataclass
class TradeRecord:
    trade_id: str
    mode: str
    slot_slug: str
    question: str
    condition_id: str
    side: str
    token_id: str
    end_time: str
    shares: float
    status: str = "open"
    entry_price: float = 0.0
    entry_notional_usdc: float = 0.0
    entry_fee_usdc: float = 0.0
    exit_price: float = 0.0
    exit_notional_usdc: float = 0.0
    exit_fee_usdc: float = 0.0
    pnl_usdc: float = 0.0
    pnl_pct: float = 0.0
    opened_at: str = ""
    closed_at: str = ""
    exit_reason: str = ""
    resolution_outcome: str = ""
    entry_order_id: str = ""
    exit_order_id: str = ""
    live_order_status: str = ""
    synced_size: bool = False
    redeemable: bool = False
    notes: list[str] = field(default_factory=list)


@dataclass
class LogEntry:
    ts: str
    level: str
    message: str
    extra: dict = field(default_factory=dict)
