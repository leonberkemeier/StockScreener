#!/usr/bin/env python3
"""
Historical Data Fetcher - Backfills 1 year of historical data for all stocks.
Run this once to populate the database, then use fetch_data.py for daily updates.
"""

import time
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List
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
    if not price or price == 0:
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


def fetch_stock_historical(ticker: str, eur_usd_rate: float) -> Optional[Dict]:
    """Fetch 1 year of historical data for a stock."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get 1 year of historical data
        hist = stock.history(period="1y")
        if hist.empty:
            return None
        
        # Get basic info
        currency = info.get('currency', 'USD')
        
        # Calculate volatility (annualized)
        returns = hist['Close'].pct_change().dropna()
        volatility = returns.std() * (252 ** 0.5) if len(returns) > 0 else 0
        
        # Get static info
        stock_info = {
            'ticker': ticker,
            'name': info.get('longName', ticker),
            'sector': info.get('sector', 'Unknown'),
            'country': info.get('country', 'Unknown'),
            'currency': currency,
            'exchange': info.get('exchange', '')
        }
        
        # Get current valuation metrics
        dividend_yield = info.get('dividendYield', 0) or 0
        payout_ratio = info.get('payoutRatio', 0) or 0
        beta = info.get('beta', 0) or 0
        pe_ratio = info.get('trailingPE', 0) or 0
        pb_ratio = info.get('priceToBook', 0) or 0
        market_cap = info.get('marketCap', 0)
        
        # Convert market cap to EUR
        market_cap_eur = convert_to_eur(market_cap, currency, eur_usd_rate) if market_cap else None
        
        # Process each day of historical data
        historical_data = []
        
        for date, row in hist.iterrows():
            price = row['Close']
            volume = row['Volume']
            day_high = row['High']
            day_low = row['Low']
            
            # Convert prices to EUR
            price_eur = convert_to_eur(price, currency, eur_usd_rate)
            day_high_eur = convert_to_eur(day_high, currency, eur_usd_rate)
            day_low_eur = convert_to_eur(day_low, currency, eur_usd_rate)
            
            if price_eur:
                # Get year high/low up to this date
                hist_up_to_date = hist.loc[:date]
                year_high = hist_up_to_date['High'].max()
                year_low = hist_up_to_date['Low'].min()
                
                year_high_eur = convert_to_eur(year_high, currency, eur_usd_rate)
                year_low_eur = convert_to_eur(year_low, currency, eur_usd_rate)
                
                data_point = {
                    'ticker': ticker,
                    'date': date.strftime('%Y-%m-%d'),
                    'price_eur': round(price_eur, 2),
                    'market_cap_eur': market_cap_eur,
                    'dividend_yield': dividend_yield,
                    'payout_ratio': payout_ratio,
                    'beta': beta,
                    'pe_ratio': pe_ratio,
                    'pb_ratio': pb_ratio,
                    'volatility': round(volatility, 4),
                    'volume': int(volume) if volume else 0,
                    'day_high_eur': round(day_high_eur, 2) if day_high_eur else None,
                    'day_low_eur': round(day_low_eur, 2) if day_low_eur else None,
                    'year_high_eur': round(year_high_eur, 2) if year_high_eur else None,
                    'year_low_eur': round(year_low_eur, 2) if year_low_eur else None
                }
                
                historical_data.append(data_point)
        
        return {
            'info': stock_info,
            'historical_data': historical_data
        }
        
    except Exception as e:
        print(f"  Error fetching {ticker}: {str(e)[:80]}")
        return None


def main():
    print("=" * 60)
    print("Historical Data Fetcher (1 Year Backfill)")
    print("=" * 60)
    
    start_time = time.time()
    
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
    print(f"\nFetching 1 year of historical data for {len(tickers)} stocks...")
    print("This will take 8-10 minutes with rate limiting...")
    print("Progress will be slower due to processing ~365 days per stock\n")
    
    success_count = 0
    fail_count = 0
    total_data_points = 0
    failed_tickers = []  # Track failed tickers
    
    for i, ticker in enumerate(tickers):
        # Progress update
        if i % 25 == 0 and i > 0:
            elapsed = time.time() - start_time
            progress = i / len(tickers) * 100
            eta = (elapsed / i) * (len(tickers) - i)
            print(f"Progress: {i}/{len(tickers)} ({progress:.1f}%) - "
                  f"Success: {success_count} | Failed: {fail_count} | "
                  f"Data points: {total_data_points:,} | "
                  f"ETA: {eta/60:.1f} min")
        
        # Fetch historical data
        result = fetch_stock_historical(ticker, eur_rate)
        
        if result and result['historical_data']:
            # Add stock to master list
            db.add_stock(
                result['info']['ticker'],
                result['info']['name'],
                result['info']['sector'],
                result['info']['country'],
                result['info']['currency'],
                result['info']['exchange']
            )
            
            # Add all historical data points
            for data_point in result['historical_data']:
                db.add_stock_data(data_point)
                total_data_points += 1
            
            success_count += 1
        else:
            fail_count += 1
            failed_tickers.append(ticker)  # Record failure
        
        # Rate limiting
        time.sleep(0.5)
    
    # Final stats
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("Historical Data Fetch Complete")
    print("=" * 60)
    print(f"Total tickers: {len(tickers)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Success rate: {success_count/len(tickers)*100:.1f}%")
    print(f"Total data points stored: {total_data_points:,}")
    print(f"Average days per stock: {total_data_points/success_count:.0f}")
    print(f"Execution time: {elapsed/60:.1f} minutes")
    print("=" * 60)
    
    # Database stats
    stats = db.get_stats()
    print(f"\nDatabase Stats:")
    print(f"  Total stocks: {stats['total_stocks']}")
    print(f"  Latest data: {stats['latest_data_date']}")
    print(f"  Total data points: {stats['total_data_points']:,}")
    print(f"  Database size: ~{stats['total_data_points'] * 0.001:.1f} MB")
    
    db.close()
    
    # Save failed tickers to file
    if failed_tickers:
        failed_file = Path(__file__).parent / 'output' / 'failed_tickers.txt'
        failed_file.parent.mkdir(exist_ok=True)
        
        with open(failed_file, 'w') as f:
            f.write(f"# Failed tickers from historical fetch - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Total failed: {len(failed_tickers)}\n\n")
            for ticker in failed_tickers:
                f.write(f"{ticker}\n")
        
        print(f"\n✓ Failed tickers saved to: {failed_file}")
        print(f"  Total failed: {len(failed_tickers)}")
        print(f"  Reasons: Delisted, no data, API errors, currency not supported")
    
    print("\n✓ Historical data backfill complete!")
    print("  You can now run screener.py to find opportunities.")
    print("  Use fetch_data.py for daily updates going forward.")


if __name__ == '__main__':
    main()
