# Kraken Paper Trading Bot Starter

This is a starter paper trading bot in Python that uses Kraken public market data and simulates trades locally.

## What it does
- Pulls OHLC candles from Kraken public REST API
- Computes a simple SMA crossover strategy
- Simulates long-only paper trades
- Logs trades to `trades.csv`

## Install
```bash
pip install -r requirements.txt
