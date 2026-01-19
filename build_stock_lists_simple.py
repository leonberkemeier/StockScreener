#!/usr/bin/env python3
"""
Simplified Stock List Builder - Uses a master ticker list file.
"""

import json
import time
import yfinance as yf
from typing import List, Dict
from pathlib import Path


def load_master_tickers() -> List[str]:
    """Load all tickers from master list."""
    ticker_file = Path(__file__).parent / 'config' / 'master_tickers.txt'
    with open(ticker_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def get_eur_rate() -> float:
    """Get EUR/USD exchange rate."""
    try:
        eurusd = yf.Ticker("EURUSD=X")
        rate = 1 / eurusd.history(period="1d")['Close'].iloc[-1]
        return rate
    except:
        return 0.92  # Fallback


def get_stock_data(ticker: str, eur_usd_rate: float) -> Dict:
    """Fetch stock data from yfinance and convert to EUR."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get historical data for volatility
        hist = stock.history(period="1y")
        if hist.empty:
            return None
        
        # Calculate volatility
        returns = hist['Close'].pct_change().dropna()
        volatility = returns.std() * (252 ** 0.5)
        
        # Get basic info
        currency = info.get('currency', 'USD')
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        market_cap = info.get('marketCap')
        
        if not price or not market_cap:
            return None
        
        # Convert to EUR
        if currency == 'USD':
            price_eur = price * eur_usd_rate
            market_cap_eur = market_cap * eur_usd_rate
        elif currency == 'EUR':
            price_eur = price
            market_cap_eur = market_cap
        elif currency == 'GBP':
            price_eur = price * 1.17
            market_cap_eur = market_cap * 1.17
        elif currency == 'CHF':
            price_eur = price * 1.05
            market_cap_eur = market_cap * 1.05
        elif currency == 'SEK':
            price_eur = price * 0.091
            market_cap_eur = market_cap * 0.091
        elif currency == 'NOK':
            price_eur = price * 0.088
            market_cap_eur = market_cap * 0.088
        elif currency == 'DKK':
            price_eur = price * 0.134
            market_cap_eur = market_cap * 0.134
        else:
            return None
        
        return {
            'ticker': ticker,
            'name': info.get('longName', ticker),
            'sector': info.get('sector', 'Unknown'),
            'country': info.get('country', 'Unknown'),
            'price_eur': round(price_eur, 2),
            'market_cap_eur': market_cap_eur,
            'dividend_yield': info.get('dividendYield', 0) or 0,
            'payout_ratio': info.get('payoutRatio', 0) or 0,
            'beta': info.get('beta', 0) or 0,
            'pe_ratio': info.get('trailingPE', 0) or 0,
            'pb_ratio': info.get('priceToBook', 0) or 0,
            'volatility': round(volatility, 4),
            'currency': currency
        }
    except Exception as e:
        return None


def screen_dividend_stocks(stocks: List[Dict], target: int = 400) -> List[Dict]:
    """Screen for dividend stocks."""
    # Very relaxed criteria - any stock with dividends
    filtered = [
        s for s in stocks
        if s['dividend_yield'] > 0
        and s['market_cap_eur'] > 100_000_000  # Lower to €100M
    ]
    
    # Sort by yield
    filtered.sort(key=lambda x: x['dividend_yield'], reverse=True)
    
    # Take top with diversity
    selected = []
    countries = {}
    for stock in filtered:
        country = stock['country']
        if countries.get(country, 0) < target * 0.3:
            selected.append(stock)
            countries[country] = countries.get(country, 0) + 1
            if len(selected) >= target:
                break
    
    return selected


def screen_volatility_stocks(stocks: List[Dict], target: int = 400) -> List[Dict]:
    """Screen for volatility stocks."""
    target_sectors = ['Technology', 'Healthcare', 'Biotechnology',
                      'Energy', 'Basic Materials', 'Consumer Cyclical', 'Communication Services']
    
    # Relaxed criteria
    filtered = [
        s for s in stocks
        if (s['beta'] > 1.2 or s['sector'] in target_sectors)  # Lower beta threshold
        and s['market_cap_eur'] > 100_000_000  # Lower market cap
        and s['volatility'] > 0.15  # Lower volatility threshold
    ]
    
    # Calculate combined score
    for stock in filtered:
        stock['volatility_score'] = stock['beta'] + stock['volatility']
    
    filtered.sort(key=lambda x: x['volatility_score'], reverse=True)
    
    # Take top with sector diversity
    selected = []
    sectors = {}
    for stock in filtered:
        sector = stock['sector']
        if sectors.get(sector, 0) < target * 0.25:
            selected.append(stock)
            sectors[sector] = sectors.get(sector, 0) + 1
            if len(selected) >= target:
                break
    
    return selected


def main():
    print("=" * 60)
    print("Stock List Builder (Simplified)")
    print("=" * 60)
    
    # Load tickers
    print("\nLoading master ticker list...")
    tickers = load_master_tickers()
    print(f"Loaded {len(tickers)} tickers")
    
    # Get EUR rate
    print("\nFetching EUR/USD rate...")
    eur_rate = get_eur_rate()
    print(f"EUR/USD rate: {eur_rate:.4f}")
    
    # Fetch data
    print(f"\nFetching data for {len(tickers)} stocks...")
    print("This will take 5-10 minutes with rate limiting...")
    
    stocks = []
    for i, ticker in enumerate(tickers):
        if i % 50 == 0:
            print(f"Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%)")
        
        data = get_stock_data(ticker, eur_rate)
        if data:
            stocks.append(data)
        
        time.sleep(0.5)  # Rate limiting
    
    print(f"\nFetched data for {len(stocks)} stocks")
    
    # Save all fetched stocks first
    config_dir = Path(__file__).parent / 'config'
    with open(config_dir / 'all_stocks_raw.json', 'w') as f:
        json.dump(stocks, f, indent=2)
    print("Saved all stocks to all_stocks_raw.json")
    
    # Screen
    dividend_stocks = screen_dividend_stocks(stocks, 400)
    volatility_stocks = screen_volatility_stocks(stocks, 400)
    
    with open(config_dir / 'dividend_stocks.json', 'w') as f:
        json.dump(dividend_stocks, f, indent=2)
    
    with open(config_dir / 'volatility_stocks.json', 'w') as f:
        json.dump(volatility_stocks, f, indent=2)
    
    print("\n" + "=" * 60)
    print(f"✓ Dividend stocks: {len(dividend_stocks)}")
    print(f"✓ Volatility stocks: {len(volatility_stocks)}")
    print("=" * 60)


if __name__ == '__main__':
    main()
