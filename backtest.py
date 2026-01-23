#!/usr/bin/env python3
"""
Backtest stock screening strategies.
Tests if stocks identified by the screener would have been profitable investments.
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))
from common.database import StockDatabase


def load_config() -> Dict:
    """Load screening thresholds."""
    config_file = Path(__file__).parent / 'config' / 'screening_thresholds.json'
    with open(config_file, 'r') as f:
        return json.load(f)


def get_available_dates(db: StockDatabase, min_days_back: int = 180) -> List[str]:
    """Get list of dates we can backtest (must have enough history before them)."""
    cutoff = datetime.now() - timedelta(days=min_days_back)
    
    cursor = db.cursor
    cursor.execute("""
        SELECT DISTINCT date 
        FROM stock_data 
        WHERE date <= ?
        ORDER BY date DESC
    """, (cutoff.strftime('%Y-%m-%d'),))
    
    return [row[0] for row in cursor.fetchall()]


def screen_on_date(db: StockDatabase, config: Dict, target_date: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Run screening as if we were on target_date.
    Returns (dividend_opportunities, volatility_opportunities)
    """
    print(f"\n  Screening as of {target_date}...")
    
    dividend_opps = []
    volatility_opps = []
    
    thresholds_div = config['dividend']
    thresholds_vol = config['volatility']
    
    # Get all stocks with data on target date
    cursor = db.cursor
    cursor.execute("""
        SELECT sd.*, s.name, s.sector, s.country
        FROM stock_data sd
        JOIN stocks s ON sd.ticker = s.ticker
        WHERE sd.date = ?
        AND sd.market_cap_eur > ?
    """, (target_date, thresholds_div['min_market_cap_eur']))
    
    stocks = [dict(row) for row in cursor.fetchall()]
    print(f"    Found {len(stocks)} stocks with data on {target_date}")
    
    # Screen dividend opportunities
    for stock in stocks:
        ticker = stock['ticker']
        current_yield = stock['dividend_yield']
        
        # Normalize yield
        if current_yield > 1:
            current_yield = current_yield / 100
        
        if current_yield < thresholds_div['min_current_yield']:
            continue
        
        current_price = stock['price_eur']
        if not current_price or current_price <= 0:
            continue
        
        # Get 90 days of history BEFORE target_date
        cursor.execute("""
            SELECT price_eur, date
            FROM stock_data
            WHERE ticker = ?
            AND date < ?
            AND date >= date(?, '-90 days')
            ORDER BY date DESC
        """, (ticker, target_date, target_date))
        
        history = [dict(row) for row in cursor.fetchall()]
        
        if len(history) < 30:
            continue
        
        # Calculate metrics
        prices = [h['price_eur'] for h in history if h['price_eur']]
        if not prices:
            continue
        
        price_90d_avg = sum(prices) / len(prices)
        dividend_per_share = current_yield * current_price
        historical_yield = dividend_per_share / price_90d_avg if price_90d_avg > 0 else 0
        yield_expansion = current_yield - historical_yield
        price_discount = (price_90d_avg - current_price) / price_90d_avg
        
        # Check criteria
        if (yield_expansion >= thresholds_div['min_yield_expansion_pp'] and
            price_discount >= thresholds_div['min_price_discount_vs_90d_avg'] and
            stock['pe_ratio'] > 0 and stock['pe_ratio'] <= thresholds_div['max_pe_ratio']):
            
            dividend_opps.append({
                'ticker': ticker,
                'name': stock['name'],
                'sector': stock['sector'],
                'entry_date': target_date,
                'entry_price': current_price,
                'yield_expansion': yield_expansion,
                'price_discount': price_discount,
                'pe_ratio': stock['pe_ratio']
            })
    
    # Screen volatility opportunities
    for stock in stocks:
        ticker = stock['ticker']
        
        if stock['market_cap_eur'] < thresholds_vol['min_market_cap_eur']:
            continue
        
        if not (stock['beta'] >= thresholds_vol['min_beta'] or 
                stock['volatility'] >= thresholds_vol['min_volatility']):
            continue
        
        # Get 90-day high before target_date
        cursor.execute("""
            SELECT MAX(price_eur) as high_90d
            FROM stock_data
            WHERE ticker = ?
            AND date < ?
            AND date >= date(?, '-90 days')
        """, (ticker, target_date, target_date))
        
        result = cursor.fetchone()
        high_90d = result[0] if result else None
        
        if not high_90d or not stock['price_eur']:
            continue
        
        drop_from_high = (stock['price_eur'] - high_90d) / high_90d
        
        if drop_from_high <= -thresholds_vol['min_drop_from_high']:
            if stock['pe_ratio'] > 0 and stock['pe_ratio'] > thresholds_vol['max_pe_ratio']:
                continue
            
            volatility_opps.append({
                'ticker': ticker,
                'name': stock['name'],
                'sector': stock['sector'],
                'entry_date': target_date,
                'entry_price': stock['price_eur'],
                'drop_from_high': drop_from_high,
                'beta': stock['beta'],
                'pe_ratio': stock['pe_ratio']
            })
    
    print(f"    Found {len(dividend_opps)} dividend + {len(volatility_opps)} volatility opportunities")
    return dividend_opps, volatility_opps


def calculate_returns(db: StockDatabase, opportunities: List[Dict], exit_date: str) -> List[Dict]:
    """Calculate returns if we bought at entry_date and sold at exit_date."""
    results = []
    
    cursor = db.cursor
    for opp in opportunities:
        ticker = opp['ticker']
        entry_price = opp['entry_price']
        
        # Get exit price
        cursor.execute("""
            SELECT price_eur
            FROM stock_data
            WHERE ticker = ?
            AND date <= ?
            ORDER BY date DESC
            LIMIT 1
        """, (ticker, exit_date))
        
        result = cursor.fetchone()
        if not result or not result[0]:
            continue
        
        exit_price = result[0]
        
        returns_pct = ((exit_price - entry_price) / entry_price) * 100
        
        results.append({
            **opp,
            'exit_date': exit_date,
            'exit_price': exit_price,
            'return_pct': returns_pct
        })
    
    return results


def run_backtest(months_back: int = 6, holding_period_days: int = 180):
    """
    Run backtest starting from months_back ago with holding_period_days.
    
    Args:
        months_back: How many months ago to start backtest (default: 6)
        holding_period_days: How long to hold stocks (default: 180 days)
    """
    print("=" * 80)
    print("STOCK SCREENER BACKTEST")
    print("=" * 80)
    
    config = load_config()
    db = StockDatabase()
    db.connect()
    
    try:
        # Calculate dates
        target_entry = datetime.now() - timedelta(days=months_back * 30)
        
        # Check if we have data
        cursor = db.cursor
        cursor.execute("SELECT MIN(date), MAX(date) FROM stock_data")
        min_date, max_date = cursor.fetchone()
        
        print(f"\nData Availability:")
        print(f"  Database Range: {min_date} to {max_date}")
        
        # Find nearest trading day to target
        cursor.execute("""
            SELECT date, COUNT(DISTINCT ticker) as num_stocks
            FROM stock_data
            WHERE date >= ?
            GROUP BY date
            HAVING num_stocks > 500
            ORDER BY date ASC
            LIMIT 1
        """, (target_entry.strftime('%Y-%m-%d'),))
        
        result = cursor.fetchone()
        if not result:
            print(f"\n❌ Error: No trading day found after {target_entry.strftime('%Y-%m-%d')}")
            print(f"   Try a more recent backtest (fewer months_back)")
            return
        
        entry_date = result[0]
        exit_date = (datetime.strptime(entry_date, '%Y-%m-%d') + 
                    timedelta(days=holding_period_days)).strftime('%Y-%m-%d')
        
        # Find nearest trading day for exit
        cursor.execute("""
            SELECT date
            FROM stock_data
            WHERE date <= ?
            GROUP BY date
            ORDER BY date DESC
            LIMIT 1
        """, (exit_date,))
        
        result = cursor.fetchone()
        if result:
            exit_date = result[0]
        
        print(f"\nBacktest Parameters:")
        print(f"  Entry Date: {entry_date} (nearest trading day ~{months_back} months ago)")
        print(f"  Exit Date: {exit_date} (after ~{holding_period_days} days)")
        print(f"  Holding Period: {holding_period_days} days ({holding_period_days/30:.1f} months)")
        
        # Run screening on entry date
        print(f"\n{'=' * 80}")
        print("STEP 1: Run Screening on Entry Date")
        print('=' * 80)
        
        dividend_opps, volatility_opps = screen_on_date(db, config, entry_date)
        
        if not dividend_opps and not volatility_opps:
            print(f"\n⚠️  No opportunities found on {entry_date}")
            print("   Try adjusting thresholds in config/screening_thresholds.json")
            return
        
        # Calculate returns
        print(f"\n{'=' * 80}")
        print("STEP 2: Calculate Returns")
        print('=' * 80)
        
        dividend_results = calculate_returns(db, dividend_opps, exit_date)
        volatility_results = calculate_returns(db, volatility_opps, exit_date)
        
        # Generate report
        print(f"\n{'=' * 80}")
        print("BACKTEST RESULTS")
        print('=' * 80)
        
        print(f"\n📊 DIVIDEND STRATEGY")
        print("-" * 80)
        if dividend_results:
            print(f"Total Opportunities: {len(dividend_results)}")
            
            winning = [r for r in dividend_results if r['return_pct'] > 0]
            losing = [r for r in dividend_results if r['return_pct'] <= 0]
            
            avg_return = sum(r['return_pct'] for r in dividend_results) / len(dividend_results)
            median_return = sorted(r['return_pct'] for r in dividend_results)[len(dividend_results)//2]
            best = max(dividend_results, key=lambda x: x['return_pct'])
            worst = min(dividend_results, key=lambda x: x['return_pct'])
            
            print(f"Winners: {len(winning)} ({len(winning)/len(dividend_results)*100:.1f}%)")
            print(f"Losers: {len(losing)} ({len(losing)/len(dividend_results)*100:.1f}%)")
            print(f"\nAverage Return: {avg_return:.2f}%")
            print(f"Median Return: {median_return:.2f}%")
            print(f"Best: {best['ticker']} ({best['name']}) +{best['return_pct']:.2f}%")
            print(f"Worst: {worst['ticker']} ({worst['name']}) {worst['return_pct']:.2f}%")
            
            print(f"\nTop 5 Performers:")
            for r in sorted(dividend_results, key=lambda x: x['return_pct'], reverse=True)[:5]:
                print(f"  {r['ticker']:10} {r['name'][:30]:30} "
                      f"€{r['entry_price']:.2f} → €{r['exit_price']:.2f} "
                      f"({r['return_pct']:+.2f}%)")
        else:
            print("No dividend opportunities with exit prices available")
        
        print(f"\n⚡ VOLATILITY STRATEGY")
        print("-" * 80)
        if volatility_results:
            print(f"Total Opportunities: {len(volatility_results)}")
            
            winning = [r for r in volatility_results if r['return_pct'] > 0]
            losing = [r for r in volatility_results if r['return_pct'] <= 0]
            
            avg_return = sum(r['return_pct'] for r in volatility_results) / len(volatility_results)
            median_return = sorted(r['return_pct'] for r in volatility_results)[len(volatility_results)//2]
            best = max(volatility_results, key=lambda x: x['return_pct'])
            worst = min(volatility_results, key=lambda x: x['return_pct'])
            
            print(f"Winners: {len(winning)} ({len(winning)/len(volatility_results)*100:.1f}%)")
            print(f"Losers: {len(losing)} ({len(losing)/len(volatility_results)*100:.1f}%)")
            print(f"\nAverage Return: {avg_return:.2f}%")
            print(f"Median Return: {median_return:.2f}%")
            print(f"Best: {best['ticker']} ({best['name']}) +{best['return_pct']:.2f}%")
            print(f"Worst: {worst['ticker']} ({worst['name']}) {worst['return_pct']:.2f}%")
            
            print(f"\nTop 5 Performers:")
            for r in sorted(volatility_results, key=lambda x: x['return_pct'], reverse=True)[:5]:
                print(f"  {r['ticker']:10} {r['name'][:30]:30} "
                      f"€{r['entry_price']:.2f} → €{r['exit_price']:.2f} "
                      f"({r['return_pct']:+.2f}%)")
        else:
            print("No volatility opportunities with exit prices available")
        
        # Save detailed results
        output_dir = Path(__file__).parent / 'output'
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if dividend_results:
            output_file = output_dir / f'backtest_dividend_{timestamp}.json'
            with open(output_file, 'w') as f:
                json.dump(dividend_results, f, indent=2)
            print(f"\n💾 Dividend results saved to: {output_file}")
        
        if volatility_results:
            output_file = output_dir / f'backtest_volatility_{timestamp}.json'
            with open(output_file, 'w') as f:
                json.dump(volatility_results, f, indent=2)
            print(f"💾 Volatility results saved to: {output_file}")
        
        print("\n" + "=" * 80)
        
    finally:
        db.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Backtest stock screening strategies')
    parser.add_argument('--months-back', type=int, default=6,
                       help='How many months ago to start (default: 6)')
    parser.add_argument('--holding-period', type=int, default=180,
                       help='Holding period in days (default: 180)')
    
    args = parser.parse_args()
    
    run_backtest(months_back=args.months_back, holding_period_days=args.holding_period)
