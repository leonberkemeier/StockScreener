#!/usr/bin/env python3
"""Re-screen using already fetched data."""

import json
from pathlib import Path
from build_stock_lists_simple import screen_dividend_stocks, screen_volatility_stocks

# First, let's create a full list from the last run
# We'll need to run the full script once to generate all_stocks_raw.json
config_dir = Path(__file__).parent / 'config'

# Load all stocks
try:
    with open(config_dir / 'all_stocks_raw.json', 'r') as f:
        all_stocks = json.load(f)
    print(f"Loaded {len(all_stocks)} stocks from cache")
except FileNotFoundError:
    print("Error: all_stocks_raw.json not found. Run build_stock_lists_simple.py first.")
    exit(1)

# Screen
dividend_stocks = screen_dividend_stocks(all_stocks, 400)
volatility_stocks = screen_volatility_stocks(all_stocks, 400)

# Save
with open(config_dir / 'dividend_stocks.json', 'w') as f:
    json.dump(dividend_stocks, f, indent=2)

with open(config_dir / 'volatility_stocks.json', 'w') as f:
    json.dump(volatility_stocks, f, indent=2)

print(f"\n✓ Dividend stocks: {len(dividend_stocks)}")
print(f"✓ Volatility stocks: {len(volatility_stocks)}")
