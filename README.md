# Kraken Paper Trading Bot

A Python paper-trading bot that uses Kraken public OHLC data with strict risk controls.

## Features
- Uses **Kraken public market data** (`/0/public/OHLC`).
- Starts with **$100,000** paper cash balance.
- Uses SMA crossover entries/exits.
- Applies stop-loss and take-profit on every trade with a max **1:3 risk:reward** ratio.
- Ensures per-trade risk never exceeds **10% of account equity**.
- Restricts entries to **Monday-Friday** during **9:30am-4:00pm ET**.
- Stops trading for the day after reaching **10% daily profit target**.
- Sends a notification (console + optional webhook) for every completed trade.
- Logs all trade fills to `trades.csv`.
- Prints live status every poll so you can watch trades being set in real time.

## Install
```bash
pip install -r requirements.txt
```

## Run
```bash
python bot.py
```

## Optional notifications
Set `NOTIFICATION_WEBHOOK_URL` in `config.py` to send JSON notifications to your webhook endpoint.

## Configuration
All trading settings are in `config.py` (Kraken pair, interval, SMA windows, risk controls, and polling interval).
