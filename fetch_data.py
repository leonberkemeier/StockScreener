#!/usr/bin/env python3
"""
Daily Data Fetcher - Fetches stock data for all tickers and stores in database.
"""

import time
import yfinance as yf
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import sys

# Add common to path
sys.path.insert(0, str(Path(__file__).parent))
from common.database import StockDatabase


def load_tickers() -> list[str]:
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
    except Exception as e:
        print(f"Warning: Could not fetch EUR/USD rate: {e}")
        return 0.92  # Fallback rate


def convert_to_eur(price: float, currency: str, eur_usd_rate: float) -> Optional[float]:
    """Convert price to EUR based on currency."""
    if not price:
        return None
    
    conversion_rates = {
        'USD': eur_usd_rate,
        'EUR': 1.0,
        'GBP': 1.17,
        'CHF': 1.05,
        'SEK': 0.091,
        'NOK': 0.088,
        'DKK': 0.134,
        'PLN': 0.23,
        'HUF': 0.0026,
        'CZK': 0.041,
        'TRY': 0.028,
        'BRL': 0.18,
        'INR': 0.011,
        'CNY': 0.13,
        'HKD': 0.12,
        'RUB': 0.0095
    }
    
    rate = conversion_rates.get(currency)
    if rate:
        return price * rate
    return None


def fetch_stock_data(ticker: str, eur_usd_rate: float, today: str) -> Optional[Dict]:
    """Fetch data for a single stock."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get historical data for volatility
        hist = stock.history(period="1y")
        if hist.empty:
            return None
        
        # Calculate volatility (annualized)
        returns = hist['Close'].pct_change().dropna()
        volatility = returns.std() * (252 ** 0.5) if len(returns) > 0 else 0
        
        # Get basic info
        currency = info.get('currency', 'USD')
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        market_cap = info.get('marketCap')
        
        if not price or not market_cap:
            return None
        
        # Convert to EUR
        price_eur = convert_to_eur(price, currency, eur_usd_rate)
        market_cap_eur = convert_to_eur(market_cap, currency, eur_usd_rate)
        
        if not price_eur or not market_cap_eur:
            return None
        
        # Get day high/low
        day_high = info.get('dayHigh') or hist['High'].iloc[-1] if not hist.empty else None
        day_low = info.get('dayLow') or hist['Low'].iloc[-1] if not hist.empty else None
        
        # Get 52-week high/low
        year_high = info.get('fiftyTwoWeekHigh')
        year_low = info.get('fiftyTwoWeekLow')
        
        # Convert high/low to EUR
        day_high_eur = convert_to_eur(day_high, currency, eur_usd_rate) if day_high else None
        day_low_eur = convert_to_eur(day_low, currency, eur_usd_rate) if day_low else None
        year_high_eur = convert_to_eur(year_high, currency, eur_usd_rate) if year_high else None
        year_low_eur = convert_to_eur(year_low, currency, eur_usd_rate) if year_low else None
        
        # Stock info for stocks table
        stock_info = {
            'ticker': ticker,
            'name': info.get('longName', ticker),
            'sector': info.get('sector', 'Unknown'),
            'country': info.get('country', 'Unknown'),
            'currency': currency,
            'exchange': info.get('exchange', '')
        }
        
        # Data for stock_data table
        stock_data = {
            'ticker': ticker,
            'date': today,
            'price_eur': round(price_eur, 2),
            'market_cap_eur': market_cap_eur,
            'dividend_yield': info.get('dividendYield', 0) or 0,
            'payout_ratio': info.get('payoutRatio', 0) or 0,
            'beta': info.get('beta', 0) or 0,
            'pe_ratio': info.get('trailingPE', 0) or 0,
            'pb_ratio': info.get('priceToBook', 0) or 0,
            'volatility': round(volatility, 4),
            'volume': info.get('volume', 0) or 0,
            'day_high_eur': round(day_high_eur, 2) if day_high_eur else None,
            'day_low_eur': round(day_low_eur, 2) if day_low_eur else None,
            'year_high_eur': round(year_high_eur, 2) if year_high_eur else None,
            'year_low_eur': round(year_low_eur, 2) if year_low_eur else None
        }
        
        return {'info': stock_info, 'data': stock_data}
        
    except Exception as e:
        print(f"  Error fetching {ticker}: {str(e)[:50]}")
        return None


def main():
    print("=" * 60)
    print("Stock Data Fetcher")
    print("=" * 60)
    
    start_time = time.time()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Load tickers
    print(f"\nLoading tickers...")
    tickers = load_tickers()
    print(f"Loaded {len(tickers)} tickers")
    
    # Get EUR rate
    print("\nFetching EUR/USD rate...")
    eur_rate = get_eur_rate()
    print(f"EUR/USD rate: {eur_rate:.4f}")
    
    # Connect to database
    print("\nConnecting to database...")
    db = StockDatabase()
    db.connect()
    
    # Fetch data
    print(f"\nFetching data for {len(tickers)} stocks...")
    print("This will take 6-8 minutes with rate limiting...\n")
    
    success_count = 0
    fail_count = 0
    
    for i, ticker in enumerate(tickers):
        # Progress update
        if i % 50 == 0 and i > 0:
            elapsed = time.time() - start_time
            progress = i / len(tickers) * 100
            eta = (elapsed / i) * (len(tickers) - i)
            print(f"Progress: {i}/{len(tickers)} ({progress:.1f}%) - "
                  f"Success: {success_count} | Failed: {fail_count} | "
                  f"ETA: {eta/60:.1f} min")
        
        # Fetch data
        result = fetch_stock_data(ticker, eur_rate, today)
        
        if result:
            # Add stock to master list
            db.add_stock(
                result['info']['ticker'],
                result['info']['name'],
                result['info']['sector'],
                result['info']['country'],
                result['info']['currency'],
                result['info']['exchange']
            )
            
            # Add stock data
            db.add_stock_data(result['data'])
            success_count += 1
        else:
            fail_count += 1
        
        # Rate limiting
        time.sleep(0.5)
    
    # Final stats
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("Data Fetch Complete")
    print("=" * 60)
    print(f"Total tickers: {len(tickers)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Success rate: {success_count/len(tickers)*100:.1f}%")
    print(f"Execution time: {elapsed/60:.1f} minutes")
    print("=" * 60)
    
    # Database stats
    stats = db.get_stats()
    print(f"\nDatabase Stats:")
    print(f"  Total stocks: {stats['total_stocks']}")
    print(f"  Latest data: {stats['latest_data_date']}")
    print(f"  Total data points: {stats['total_data_points']}")
    
    db.close()


if __name__ == '__main__':
    main()
