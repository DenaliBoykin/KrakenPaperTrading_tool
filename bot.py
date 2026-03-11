from __future__ import annotations

import csv
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

import config

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
US_EASTERN = ZoneInfo("America/New_York")


class StockDataClient:
    """Fetch minute candles for US stocks from Yahoo Finance."""

    def __init__(self, timeout: int = 15) -> None:
        self.timeout = timeout
        self.session = requests.Session()

    def get_ohlc(self, symbol: str, interval: str, lookback_range: str) -> pd.DataFrame:
        url = YAHOO_CHART_URL.format(symbol=symbol)
        params = {"interval": interval, "range": lookback_range}
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        payload = response.json()
        result = payload.get("chart", {}).get("result")
        if not result:
            raise RuntimeError(f"No chart data returned for symbol {symbol}: {payload}")

        chart = result[0]
        timestamps = chart.get("timestamp", [])
        quote = chart.get("indicators", {}).get("quote", [{}])[0]

        df = pd.DataFrame(
            {
                "time": pd.to_datetime(timestamps, unit="s", utc=True),
                "open": quote.get("open", []),
                "high": quote.get("high", []),
                "low": quote.get("low", []),
                "close": quote.get("close", []),
                "volume": quote.get("volume", []),
            }
        )

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df.dropna(subset=["open", "high", "low", "close"], inplace=True)
        df.reset_index(drop=True, inplace=True)

        if df.empty:
            raise RuntimeError("No valid OHLC rows available after cleanup.")

        return df


class Notifier:
    def __init__(self, webhook_url: Optional[str] = None) -> None:
        self.webhook_url = webhook_url
        self.session = requests.Session()

    def notify(self, message: str) -> None:
        print(f"[NOTIFY] {message}")
        if not self.webhook_url:
            return

        try:
            self.session.post(self.webhook_url, json={"text": message}, timeout=10)
        except Exception as exc:
            print(f"[{utc_now_iso()}] Notification error: {exc}")


@dataclass
class Position:
    side: str
    entry_price: float
    quantity: float
    entry_time: str
    stop_loss: float
    take_profit: float
    risk_per_share: float


@dataclass
class Trade:
    timestamp: str
    action: str
    price: float
    quantity: float
    cash_after: float
    equity_after: float
    note: str


class PaperBroker:
    def __init__(self, starting_cash: float, fee_rate: float) -> None:
        self.cash = starting_cash
        self.fee_rate = fee_rate
        self.position: Optional[Position] = None
        self.trade_history: list[Trade] = []

    def mark_to_market_equity(self, last_price: float) -> float:
        if self.position is None:
            return self.cash
        return self.cash + (self.position.quantity * last_price)

    def open_long(
        self,
        price: float,
        timestamp: str,
        stop_loss: float,
        take_profit: float,
        max_risk_dollars: float,
        note: str,
    ) -> bool:
        if self.position is not None:
            return False

        risk_per_share = max(price - stop_loss, 0)
        if risk_per_share <= 0:
            return False

        # Position size from risk budget, then cap to available buying power.
        qty_by_risk = max_risk_dollars / risk_per_share
        max_qty_by_cash = (self.cash * (1 - self.fee_rate)) / price
        qty = min(qty_by_risk, max_qty_by_cash)

        if qty <= 0:
            return False

        gross_cost = qty * price
        fee = gross_cost * self.fee_rate
        total_cost = gross_cost + fee
        if total_cost > self.cash:
            return False

        self.cash -= total_cost
        self.position = Position(
            side="long",
            entry_price=price,
            quantity=qty,
            entry_time=timestamp,
            stop_loss=stop_loss,
            take_profit=take_profit,
            risk_per_share=risk_per_share,
        )

        self.trade_history.append(
            Trade(
                timestamp=timestamp,
                action="BUY",
                price=price,
                quantity=qty,
                cash_after=self.cash,
                equity_after=self.mark_to_market_equity(price),
                note=note,
            )
        )
        return True

    def close_position(self, price: float, timestamp: str, note: str = "") -> Optional[dict]:
        if self.position is None:
            return None

        qty = self.position.quantity
        entry_price = self.position.entry_price

        gross_value = qty * price
        fee = gross_value * self.fee_rate
        net_value = gross_value - fee
        self.cash += net_value

        realized_pnl = (price - entry_price) * qty - fee
        self.position = None

        self.trade_history.append(
            Trade(
                timestamp=timestamp,
                action="SELL",
                price=price,
                quantity=qty,
                cash_after=self.cash,
                equity_after=self.cash,
                note=note,
            )
        )
        return {"qty": qty, "entry": entry_price, "exit": price, "realized_pnl": realized_pnl}


class SmaCrossStrategy:
    def __init__(self, short_window: int, long_window: int) -> None:
        if short_window >= long_window:
            raise ValueError("short_window must be less than long_window")
        self.short_window = short_window
        self.long_window = long_window

    def add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["sma_short"] = out["close"].rolling(self.short_window).mean()
        out["sma_long"] = out["close"].rolling(self.long_window).mean()
        return out

    def generate_signal(self, df: pd.DataFrame) -> str:
        if len(df) < self.long_window + 1:
            return "hold"

        current = df.iloc[-1]
        previous = df.iloc[-2]
        if pd.isna(current["sma_short"]) or pd.isna(current["sma_long"]):
            return "hold"
        if pd.isna(previous["sma_short"]) or pd.isna(previous["sma_long"]):
            return "hold"

        crossed_up = previous["sma_short"] <= previous["sma_long"] and current["sma_short"] > current["sma_long"]
        crossed_down = previous["sma_short"] >= previous["sma_long"] and current["sma_short"] < current["sma_long"]

        if crossed_up:
            return "buy"
        if crossed_down:
            return "sell"
        return "hold"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def market_is_open(now_utc: datetime) -> bool:
    now_et = now_utc.astimezone(US_EASTERN)
    if now_et.weekday() > 4:
        return False

    current_minutes = now_et.hour * 60 + now_et.minute
    market_open = 9 * 60 + 30
    market_close = 16 * 60
    return market_open <= current_minutes < market_close


def append_trades_to_csv(trades: list[Trade], filepath: str) -> None:
    if not trades:
        return

    try:
        with open(filepath, "x", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(trades[0]).keys()))
            writer.writeheader()
            for trade in trades:
                writer.writerow(asdict(trade))
    except FileExistsError:
        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(trades[0]).keys()))
            for trade in trades:
                writer.writerow(asdict(trade))


def print_status(price: float, signal: str, broker: PaperBroker, trading_paused: bool) -> None:
    equity = broker.mark_to_market_equity(price)
    pause_state = "PAUSED" if trading_paused else "ACTIVE"
    if broker.position:
        pos = broker.position
        pos_text = (
            f"qty={pos.quantity:.4f} entry={pos.entry_price:.2f} "
            f"sl={pos.stop_loss:.2f} tp={pos.take_profit:.2f}"
        )
    else:
        pos_text = "none"

    print(
        f"[{utc_now_iso()}] mode={pause_state} price={price:.2f} signal={signal.upper()} "
        f"cash={broker.cash:.2f} equity={equity:.2f} position={pos_text}"
    )


def run_bot(max_loops: Optional[int] = None) -> None:
    if config.RISK_REWARD_RATIO > 3:
        raise ValueError("RISK_REWARD_RATIO must be <= 3.0")

    client = StockDataClient()
    notifier = Notifier(webhook_url=config.NOTIFICATION_WEBHOOK_URL)
    broker = PaperBroker(starting_cash=config.STARTING_CASH, fee_rate=config.FEE_RATE)
    strategy = SmaCrossStrategy(short_window=config.SHORT_SMA, long_window=config.LONG_SMA)

    print("Starting stock paper trading bot...")
    print(f"Symbol: {config.SYMBOL}, interval={config.INTERVAL}, starting_cash=${config.STARTING_CASH:,.2f}")

    current_day = None
    day_start_equity = config.STARTING_CASH
    daily_goal_hit = False
    loops = 0

    while True:
        if max_loops is not None and loops >= max_loops:
            print("Reached max loops. Exiting.")
            break

        now_utc = datetime.now(timezone.utc)
        now_et = now_utc.astimezone(US_EASTERN)
        if now_et.date() != current_day:
            current_day = now_et.date()
            day_start_equity = broker.mark_to_market_equity(broker.position.entry_price) if broker.position else broker.cash
            daily_goal_hit = False
            print(f"[{utc_now_iso()}] New trading day. Starting equity: {day_start_equity:.2f}")

        try:
            df = client.get_ohlc(config.SYMBOL, config.INTERVAL, config.LOOKBACK_RANGE)
            df = strategy.add_indicators(df)
            latest = df.iloc[-1]
            latest_price = float(latest["close"])
            latest_time = latest["time"].isoformat()

            equity = broker.mark_to_market_equity(latest_price)
            if equity >= day_start_equity * (1 + config.DAILY_PROFIT_TARGET_PCT):
                daily_goal_hit = True

            trading_window_open = market_is_open(now_utc)
            trading_paused = (not trading_window_open) or daily_goal_hit

            signal = strategy.generate_signal(df)
            before_count = len(broker.trade_history)

            # Risk exits always checked if in position.
            if broker.position is not None:
                if latest_price <= broker.position.stop_loss:
                    trade = broker.close_position(latest_price, latest_time, note="Stop loss hit")
                    notifier.notify(f"Trade completed: STOP LOSS. PnL=${trade['realized_pnl']:.2f}")
                elif latest_price >= broker.position.take_profit:
                    trade = broker.close_position(latest_price, latest_time, note="Take profit hit")
                    notifier.notify(f"Trade completed: TAKE PROFIT. PnL=${trade['realized_pnl']:.2f}")
                elif signal == "sell":
                    trade = broker.close_position(latest_price, latest_time, note="SMA exit")
                    notifier.notify(f"Trade completed: SIGNAL EXIT. PnL=${trade['realized_pnl']:.2f}")

            if not trading_paused and signal == "buy" and broker.position is None:
                stop_loss = latest_price * (1 - config.STOP_LOSS_PCT)
                risk = latest_price - stop_loss
                take_profit = latest_price + (risk * config.RISK_REWARD_RATIO)
                max_risk_dollars = broker.mark_to_market_equity(latest_price) * config.MAX_RISK_PER_TRADE

                opened = broker.open_long(
                    price=latest_price,
                    timestamp=latest_time,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    max_risk_dollars=max_risk_dollars,
                    note="SMA cross up",
                )
                if opened:
                    print(
                        f"[{utc_now_iso()}] OPENED long qty={broker.position.quantity:.4f} "
                        f"entry={latest_price:.2f} sl={stop_loss:.2f} tp={take_profit:.2f}"
                    )

            if daily_goal_hit and broker.position is not None:
                trade = broker.close_position(latest_price, latest_time, note="Daily target reached")
                notifier.notify(f"Trade completed: DAILY TARGET LOCK. PnL=${trade['realized_pnl']:.2f}")

            after_count = len(broker.trade_history)
            append_trades_to_csv(broker.trade_history[before_count:after_count], config.TRADE_LOG_FILE)
            print_status(latest_price, signal, broker, trading_paused=trading_paused)

        except KeyboardInterrupt:
            print("\nBot stopped by user.")
            break
        except Exception as exc:
            print(f"[{utc_now_iso()}] Error: {exc}")

        loops += 1
        time.sleep(config.POLL_SECONDS)


def main() -> None:
    run_bot(max_loops=config.MAX_LOOPS)


if __name__ == "__main__":
    main()
