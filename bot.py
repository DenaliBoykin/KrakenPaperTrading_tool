from __future__ import annotations

import csv
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

import config


KRAKEN_BASE_URL = "https://api.kraken.com"
OHLC_ENDPOINT = "/0/public/OHLC"
ASSET_PAIRS_ENDPOINT = "/0/public/AssetPairs"


class KrakenPublicClient:
    """Minimal Kraken public REST client."""

    def __init__(self, base_url: str = KRAKEN_BASE_URL, timeout: int = 15) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.session = requests.Session()

    def get_ohlc(self, pair: str, interval: int = 60) -> pd.DataFrame:
        """
        Fetch OHLC candles from Kraken public API.

        Kraken notes:
        - /public/OHLC returns candle data
        - the last row is the current, not-yet-committed candle
        """
        url = f"{self.base_url}{OHLC_ENDPOINT}"
        params = {"pair": pair, "interval": interval}
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()

        payload = response.json()
        errors = payload.get("error", [])
        if errors:
            raise RuntimeError(f"Kraken API error: {errors}")

        result = payload["result"]
        # result contains pair key + "last"
        pair_key = next(k for k in result.keys() if k != "last")
        rows = result[pair_key]

        columns = [
            "time",
            "open",
            "high",
            "low",
            "close",
            "vwap",
            "volume",
            "count",
        ]
        df = pd.DataFrame(rows, columns=columns)

        # Type conversions
        numeric_cols = ["open", "high", "low", "close", "vwap", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)

        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)

        # Kraken says final row is current/uncommitted candle, so drop it for signals
        if len(df) > 1:
            df = df.iloc[:-1].copy()

        df.reset_index(drop=True, inplace=True)
        return df

    def get_asset_pairs(self) -> dict:
        """Useful helper for Codex to extend pair discovery/validation."""
        url = f"{self.base_url}{ASSET_PAIRS_ENDPOINT}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        payload = response.json()
        errors = payload.get("error", [])
        if errors:
            raise RuntimeError(f"Kraken API error: {errors}")

        return payload["result"]


@dataclass
class Position:
    side: str
    entry_price: float
    quantity: float
    entry_time: str


@dataclass
class Trade:
    timestamp: str
    action: str
    price: float
    quantity: float
    cash_after: float
    position_qty_after: float
    note: str


class PaperBroker:
    """Local paper trading simulator."""

    def __init__(self, starting_cash: float, fee_rate: float) -> None:
        self.cash = starting_cash
        self.fee_rate = fee_rate
        self.position: Optional[Position] = None
        self.trade_history: list[Trade] = []

    def mark_to_market_equity(self, last_price: float) -> float:
        if self.position is None:
            return self.cash
        return self.cash + (self.position.quantity * last_price)

    def buy_with_usd(self, usd_amount: float, price: float, timestamp: str, note: str = "") -> None:
        if self.position is not None:
            return  # single-position model for starter bot

        if usd_amount > self.cash:
            usd_amount = self.cash

        if usd_amount <= 0:
            return

        fee = usd_amount * self.fee_rate
        net_usd = usd_amount - fee
        qty = net_usd / price

        self.cash -= usd_amount
        self.position = Position(
            side="long",
            entry_price=price,
            quantity=qty,
            entry_time=timestamp,
        )

        self.trade_history.append(
            Trade(
                timestamp=timestamp,
                action="BUY",
                price=price,
                quantity=qty,
                cash_after=self.cash,
                position_qty_after=qty,
                note=note,
            )
        )

    def sell_all(self, price: float, timestamp: str, note: str = "") -> None:
        if self.position is None:
            return

        gross_value = self.position.quantity * price
        fee = gross_value * self.fee_rate
        net_value = gross_value - fee

        qty = self.position.quantity
        self.cash += net_value
        self.position = None

        self.trade_history.append(
            Trade(
                timestamp=timestamp,
                action="SELL",
                price=price,
                quantity=qty,
                cash_after=self.cash,
                position_qty_after=0.0,
                note=note,
            )
        )

    def latest_position_qty(self) -> float:
        return 0.0 if self.position is None else self.position.quantity


class SmaCrossStrategy:
    """Very simple starter strategy: short SMA crossing long SMA."""

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
        """
        Returns: 'buy', 'sell', or 'hold'
        """
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


def print_status(price: float, signal: str, broker: PaperBroker) -> None:
    equity = broker.mark_to_market_equity(price)
    pos_qty = broker.latest_position_qty()
    print(
        f"[{utc_now_iso()}] "
        f"price={price:.2f} signal={signal.upper()} "
        f"cash={broker.cash:.2f} pos_qty={pos_qty:.8f} equity={equity:.2f}"
    )


def main() -> None:
    client = KrakenPublicClient()
    broker = PaperBroker(
        starting_cash=config.STARTING_CASH,
        fee_rate=config.FEE_RATE,
    )
    strategy = SmaCrossStrategy(
        short_window=config.SHORT_SMA,
        long_window=config.LONG_SMA,
    )

    print("Starting Kraken paper trading bot...")
    print(f"Pair: {config.PAIR}, Interval: {config.INTERVAL} min")

    while True:
        try:
            df = client.get_ohlc(pair=config.PAIR, interval=config.INTERVAL)
            df = strategy.add_indicators(df)

            latest_close = float(df.iloc[-1]["close"])
            latest_time = df.iloc[-1]["time"].isoformat()
            signal = strategy.generate_signal(df)

            before_count = len(broker.trade_history)

            if signal == "buy" and broker.position is None:
                broker.buy_with_usd(
                    usd_amount=config.TRADE_SIZE_USD,
                    price=latest_close,
                    timestamp=latest_time,
                    note="SMA cross up",
                )
            elif signal == "sell" and broker.position is not None:
                broker.sell_all(
                    price=latest_close,
                    timestamp=latest_time,
                    note="SMA cross down",
                )

            after_count = len(broker.trade_history)
            new_trades = broker.trade_history[before_count:after_count]
            append_trades_to_csv(new_trades, config.TRADE_LOG_FILE)

            print_status(latest_close, signal, broker)

        except KeyboardInterrupt:
            print("\nBot stopped by user.")
            break
        except Exception as exc:
            print(f"[{utc_now_iso()}] Error: {exc}")

        time.sleep(config.POLL_SECONDS)


if __name__ == "__main__":
    main()
