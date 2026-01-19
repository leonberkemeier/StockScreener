#!/usr/bin/env python3
"""
Stock List Builder - Generates initial lists of 400 stocks for each screening strategy.

Dividend Strategy:
- Dividend yield >3%
- Consistent payout history (5+ years)
- Market cap >€1B
- Geographic diversification

Volatility Strategy:
- High beta (>1.5)
- Sector-focused (biotech, tech, clean energy, commodities)
- Geographic diversification
"""

import json
import time
import yfinance as yf
import pandas as pd
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from pathlib import Path


def fetch_sp500_tickers() -> List[str]:
    """Fetch S&P 500 constituent tickers from Wikipedia."""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url, storage_options={'User-Agent': 'Mozilla/5.0'})
    df = tables[0]
    return df['Symbol'].str.replace('.', '-').tolist()


def fetch_dax_tickers() -> List[str]:
    """Load DAX 40 tickers from manual JSON file."""
    try:
        config_path = Path(__file__).parent / 'config' / 'dax40_tickers.json'
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Could not load DAX tickers: {e}")
    return []


def fetch_cac40_tickers() -> List[str]:
    """Fetch CAC 40 tickers from Wikipedia."""
    try:
        url = 'https://en.wikipedia.org/wiki/CAC_40'
        tables = pd.read_html(url, storage_options={'User-Agent': 'Mozilla/5.0'})
        for table in tables:
            if 'Ticker' in table.columns:
                tickers = table['Ticker'].dropna().tolist()
                # Add .PA suffix for French stocks
                return [f"{t}.PA" if not t.endswith('.PA') else t for t in tickers]
    except Exception as e:
        print(f"Could not fetch CAC 40: {e}")
    return []


def fetch_nasdaq100_tickers() -> List[str]:
    """Fetch NASDAQ-100 tickers from Wikipedia."""
    try:
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        tables = pd.read_html(url, storage_options={'User-Agent': 'Mozilla/5.0'})
        for table in tables:
            if 'Ticker' in table.columns:
                return table['Ticker'].dropna().tolist()
    except Exception as e:
        print(f"Could not fetch NASDAQ-100: {e}")
    return []


def fetch_european_tickers() -> List[str]:
    """Load major European stock tickers from manual JSON file."""
    try:
        config_path = Path(__file__).parent / 'config' / 'european_tickers.json'
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Could not load European tickers: {e}")
    return []


def fetch_ftse100_tickers() -> List[str]:
    """Fetch FTSE 100 tickers from Wikipedia."""
    try:
        url = 'https://en.wikipedia.org/wiki/FTSE_100_Index'
        tables = pd.read_html(url, storage_options={'User-Agent': 'Mozilla/5.0'})
        for table in tables:
            if 'Ticker' in table.columns or 'EPIC' in table.columns:
                ticker_col = 'EPIC' if 'EPIC' in table.columns else 'Ticker'
                tickers = table[ticker_col].dropna().tolist()
                # Add .L suffix for London Stock Exchange
                return [f"{t}.L" for t in tickers]
    except Exception as e:
        print(f"Could not fetch FTSE 100: {e}")
    return []


def fetch_index_constituents() -> List[str]:
    """Fetch all major index constituents."""
    print("Fetching index constituents...")
    
    tickers = []
    
    print("- S&P 500...")
    sp500 = fetch_sp500_tickers()
    tickers.extend(sp500)
    print(f"  Found {len(sp500)} tickers")
    
    print("- FTSE 100...")
    ftse100 = fetch_ftse100_tickers()
    tickers.extend(ftse100)
    print(f"  Found {len(ftse100)} tickers")
    
    print("- DAX 40...")
    dax = fetch_dax_tickers()
    tickers.extend(dax)
    print(f"  Found {len(dax)} tickers")
    
    print("- CAC 40...")
    cac40 = fetch_cac40_tickers()
    tickers.extend(cac40)
    print(f"  Found {len(cac40)} tickers")
    
    print("- NASDAQ-100...")
    nasdaq100 = fetch_nasdaq100_tickers()
    tickers.extend(nasdaq100)
    print(f"  Found {len(nasdaq100)} tickers")
    
    print("- Major European stocks...")
    european = fetch_european_tickers()
    tickers.extend(european)
    print(f"  Found {len(european)} tickers")
    
    # Remove duplicates
    tickers = list(set(tickers))
    print(f"\nTotal unique tickers: {len(tickers)}")
    
    return tickers


def get_stock_data(ticker: str, eur_usd_rate: float) -> Dict:
    """Fetch stock data from yfinance and convert to EUR."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Get historical data for volatility calculation
        hist = stock.history(period="1y")
        if hist.empty:
            return None
        
        # Calculate volatility (annualized standard deviation of returns)
        returns = hist['Close'].pct_change().dropna()
        volatility = returns.std() * (252 ** 0.5)  # Annualized
        
        # Get currency and convert to EUR
        currency = info.get('currency', 'USD')
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        market_cap = info.get('marketCap')
        
        if not price or not market_cap:
            return None
        
        # Simple currency conversion (USD to EUR)
        if currency == 'USD':
            price_eur = price * eur_usd_rate
            market_cap_eur = market_cap * eur_usd_rate
        elif currency == 'EUR':
            price_eur = price
            market_cap_eur = market_cap
        elif currency == 'GBP':
            # Rough GBP to EUR conversion
            price_eur = price * 1.17
            market_cap_eur = market_cap * 1.17
        else:
            # Skip other currencies for now
            return None
        
        data = {
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
        
        return data
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return None


def screen_dividend_stocks(stocks: List[Dict], target_count: int = 400) -> List[Dict]:
    """Screen for dividend stocks based on criteria."""
    print("\nScreening for dividend stocks...")
    
    # Filter criteria - relaxed to get more candidates
    # Accept any stock with dividend yield > 0.01 (1%)
    filtered = [
        s for s in stocks
        if s['dividend_yield'] > 0.01  # >1% yield
        and s['market_cap_eur'] > 500_000_000  # >€500M market cap (relaxed)
        and s['dividend_yield'] < 0.20  # Sanity check: <20% yield
    ]
    
    print(f"Found {len(filtered)} stocks matching dividend criteria")
    
    # If we still don't have enough, lower the bar even further
    if len(filtered) < target_count:
        print(f"Not enough stocks, relaxing criteria further...")
        filtered = [
            s for s in stocks
            if s['dividend_yield'] > 0  # Any dividend
            and s['market_cap_eur'] > 500_000_000
            and s['dividend_yield'] < 0.20
        ]
        print(f"Found {len(filtered)} stocks with relaxed criteria")
    
    # Sort by dividend yield
    filtered.sort(key=lambda x: x['dividend_yield'], reverse=True)
    
    # Take top stocks with geographic diversity
    selected = ensure_geographic_diversity(filtered, target_count)
    
    return selected


def screen_volatility_stocks(stocks: List[Dict], target_count: int = 400) -> List[Dict]:
    """Screen for high volatility stocks based on criteria."""
    print("\nScreening for volatility stocks...")
    
    # Filter criteria: high beta OR specific sectors
    target_sectors = ['Technology', 'Healthcare', 'Biotechnology', 
                      'Energy', 'Basic Materials', 'Consumer Cyclical']
    
    filtered = [
        s for s in stocks
        if (s['beta'] > 1.5 or s['sector'] in target_sectors)
        and s['market_cap_eur'] > 500_000_000  # >€500M market cap
        and s['volatility'] > 0.2  # Reasonable volatility threshold
    ]
    
    print(f"Found {len(filtered)} stocks matching volatility criteria")
    
    # Sort by combined score (beta + volatility)
    for stock in filtered:
        stock['volatility_score'] = stock['beta'] + stock['volatility']
    
    filtered.sort(key=lambda x: x['volatility_score'], reverse=True)
    
    # Take top stocks with sector diversity
    selected = ensure_sector_diversity(filtered, target_count)
    
    return selected


def ensure_geographic_diversity(stocks: List[Dict], target: int) -> List[Dict]:
    """Ensure geographic diversity in stock selection."""
    selected = []
    countries = {}
    
    for stock in stocks:
        country = stock['country']
        count = countries.get(country, 0)
        
        # Limit per country (max 30% from one country)
        if count < target * 0.3:
            selected.append(stock)
            countries[country] = count + 1
            
            if len(selected) >= target:
                break
    
    return selected


def ensure_sector_diversity(stocks: List[Dict], target: int) -> List[Dict]:
    """Ensure sector diversity in stock selection."""
    selected = []
    sectors = {}
    
    for stock in stocks:
        sector = stock['sector']
        count = sectors.get(sector, 0)
        
        # Limit per sector (max 25% from one sector)
        if count < target * 0.25:
            selected.append(stock)
            sectors[sector] = count + 1
            
            if len(selected) >= target:
                break
    
    return selected


def main():
    print("=" * 60)
    print("Stock List Builder")
    print("=" * 60)
    
    # Get EUR/USD exchange rate
    print("\nFetching EUR/USD rate...")
    try:
        eurusd = yf.Ticker("EURUSD=X")
        eur_usd_rate = 1 / eurusd.history(period="1d")['Close'].iloc[-1]
        print(f"EUR/USD rate: {eur_usd_rate:.4f}")
    except Exception as e:
        print(f"Could not fetch EUR/USD rate, using default: {e}")
        eur_usd_rate = 0.92  # Approximate fallback
    
    # Fetch index constituents
    print("\nFetching index constituents (this may take a moment)...")
    tickers = fetch_index_constituents()
    
    if len(tickers) == 0:
        print("ERROR: No tickers found. Exiting.")
        return
    
    # Fetch stock data with rate limiting
    print("\nFetching stock data (this will take a while)...")
    stocks_data = []
    
    for i, ticker in enumerate(tickers):
        if i % 50 == 0:
            print(f"Progress: {i}/{len(tickers)} ({i/len(tickers)*100:.1f}%)")
        
        data = get_stock_data(ticker, eur_usd_rate)
        if data:
            stocks_data.append(data)
        
        # Rate limiting
        time.sleep(0.5)
    
    print(f"\nSuccessfully fetched data for {len(stocks_data)} stocks")
    
    # Screen stocks
    dividend_stocks = screen_dividend_stocks(stocks_data, 400)
    volatility_stocks = screen_volatility_stocks(stocks_data, 400)
    
    # Save to JSON files
    config_dir = Path(__file__).parent / 'config'
    config_dir.mkdir(exist_ok=True)
    
    with open(config_dir / 'dividend_stocks.json', 'w') as f:
        json.dump(dividend_stocks, f, indent=2)
    
    with open(config_dir / 'volatility_stocks.json', 'w') as f:
        json.dump(volatility_stocks, f, indent=2)
    
    print("\n" + "=" * 60)
    print("Stock lists generated successfully!")
    print(f"- Dividend stocks: {len(dividend_stocks)}")
    print(f"- Volatility stocks: {len(volatility_stocks)}")
    print("=" * 60)
    
    # Print summaries
    print("\nDividend Stocks Summary:")
    print(f"  Average yield: {sum(s['dividend_yield'] for s in dividend_stocks) / len(dividend_stocks) * 100:.2f}%")
    countries = {}
    for s in dividend_stocks:
        countries[s['country']] = countries.get(s['country'], 0) + 1
    print(f"  Countries represented: {len(countries)}")
    print(f"  Top countries: {sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5]}")
    
    print("\nVolatility Stocks Summary:")
    print(f"  Average beta: {sum(s['beta'] for s in volatility_stocks) / len(volatility_stocks):.2f}")
    sectors = {}
    for s in volatility_stocks:
        sectors[s['sector']] = sectors.get(s['sector'], 0) + 1
    print(f"  Sectors represented: {len(sectors)}")
    print(f"  Top sectors: {sorted(sectors.items(), key=lambda x: x[1], reverse=True)[:5]}")


if __name__ == '__main__':
    main()
