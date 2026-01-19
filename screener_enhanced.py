#!/usr/bin/env python3
"""
Enhanced Stock Screener - Uses historical data for yield expansion detection.
"""

import json
import time
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict

# Add common to path
sys.path.insert(0, str(Path(__file__).parent))
from common.database import StockDatabase


def load_config() -> Dict:
    """Load screening thresholds from config."""
    config_file = Path(__file__).parent / 'config' / 'screening_thresholds.json'
    with open(config_file, 'r') as f:
        return json.load(f)


def calculate_implied_historical_yield(dividend_per_share: float, history: List[Dict]) -> float:
    """Calculate what the yield would have been based on historical average price."""
    if dividend_per_share == 0:
        return 0
    
    avg_price = calculate_price_avg(history)
    if avg_price == 0:
        return 0
    
    # Implied yield = dividend / historical avg price
    return dividend_per_share / avg_price


def calculate_price_avg(history: List[Dict]) -> float:
    """Calculate average price from historical data."""
    prices = [h['price_eur'] for h in history if h['price_eur']]
    return sum(prices) / len(prices) if prices else 0


def screen_dividend_opportunities(db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Enhanced dividend screening with yield expansion detection.
    
    Finds stocks where:
    - Current yield > Historical average yield (yield expansion)
    - Price < 90-day average (discounted)
    - Fundamentally undervalued (P/E < 15)
    """
    thresholds = config['dividend']
    
    print("\nQuerying top 400 dividend stocks...")
    top_dividend = db.get_top_dividend_stocks(400)
    print(f"Found {len(top_dividend)} dividend-paying stocks")
    
    opportunities = []
    
    for stock in top_dividend:
        ticker = stock['ticker']
        # yfinance returns yield as 3.5 for 3.5%, so divide by 100 if > 1
        current_yield = stock['dividend_yield']
        if current_yield > 1:
            current_yield = current_yield / 100
        current_price = stock['price_eur']
        
        # Must meet basic criteria
        if not (current_yield >= thresholds['min_current_yield'] and
                stock['pe_ratio'] > 0 and 
                stock['pe_ratio'] <= thresholds['max_pe_ratio'] and
                stock['market_cap_eur'] >= thresholds['min_market_cap_eur']):
            continue
        
        # Get 90 days of historical data
        history = db.get_stock_history(ticker, days=90)
        
        if len(history) < 30:  # Need at least 30 days
            continue
        
        # Yield from yfinance is already a ratio (e.g., 0.035 = 3.5%)
        # Calculate what yield WOULD have been 90 days ago with same dividend
        if current_yield == 0 or current_price == 0:
            continue
        
        # Dividend amount = current yield * current price
        dividend_per_share = current_yield * current_price
        
        # Calculate what yield WOULD have been 90 days ago
        price_90d_avg = calculate_price_avg(history)
        if price_90d_avg == 0:
            continue
        
        # Historical implied yield = same dividend / old price
        historical_implied_yield = dividend_per_share / price_90d_avg
        
        # Calculate ABSOLUTE yield expansion (in percentage points)
        # Both yields are decimals (e.g., 0.035), so difference is in decimal points
        yield_expansion_pp = current_yield - historical_implied_yield
        
        # Calculate price discount vs 90-day average
        price_discount = (price_90d_avg - current_price) / price_90d_avg
        
        # Check if this is an opportunity
        if (yield_expansion_pp >= thresholds['min_yield_expansion_pp'] and
            price_discount >= thresholds['min_price_discount_vs_90d_avg']):
            
            # Check for duplicate alerts
            recent_alerts = db.get_recent_alerts(ticker, config['alerts']['duplicate_alert_days'])
            
            if not recent_alerts:
                reason = (f"Yield expanded by {yield_expansion_pp*100:.1f} percentage points "
                         f"({historical_implied_yield*100:.1f}% → {current_yield*100:.1f}%). "
                         f"Price {price_discount*100:.1f}% below 90-day average "
                         f"(€{price_90d_avg:.2f} → €{current_price:.2f}). "
                         f"Dividend: €{dividend_per_share:.2f}/share. "
                         f"P/E: {stock['pe_ratio']:.1f} (undervalued).")
                
                opportunities.append({
                    'ticker': ticker,
                    'name': stock['name'],
                    'sector': stock['sector'],
                    'country': stock['country'],
                    'price_eur': current_price,
                    'price_90d_avg': price_90d_avg,
                    'price_discount': price_discount,
                    'dividend_yield': current_yield,
                    'dividend_per_share': dividend_per_share,
                    'historical_implied_yield': historical_implied_yield,
                    'yield_expansion_pp': yield_expansion_pp,
                    'pe_ratio': stock['pe_ratio'],
                    'payout_ratio': stock['payout_ratio'],
                    'market_cap_eur': stock['market_cap_eur'],
                    'reason': reason
                })
    
    # Sort by absolute yield expansion (best opportunities first)
    opportunities.sort(key=lambda x: x['yield_expansion_pp'], reverse=True)
    
    return opportunities


def screen_volatility_opportunities(db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Screen for high volatility opportunities.
    Enhanced with longer-term price analysis.
    """
    thresholds = config['volatility']
    
    print("\nQuerying top 400 volatility stocks...")
    top_volatility = db.get_top_volatility_stocks(400)
    print(f"Found {len(top_volatility)} high-volatility stocks")
    
    opportunities = []
    
    for stock in top_volatility:
        ticker = stock['ticker']
        
        # Check basic criteria
        if (stock['market_cap_eur'] >= thresholds['min_market_cap_eur'] and
            (stock['beta'] >= thresholds['min_beta'] or 
             stock['volatility'] >= thresholds['min_volatility'])):
            
            # Check P/E if available
            if stock['pe_ratio'] > 0 and stock['pe_ratio'] > thresholds['max_pe_ratio']:
                continue
            
            # Get 90 days of history for better analysis
            history = db.get_stock_history(ticker, days=90)
            
            if len(history) < 30:
                continue
            
            # Calculate 90-day high
            high_90d = max(h['price_eur'] for h in history if h['price_eur'])
            
            # Check for dip from 90-day high
            if high_90d and stock['price_eur']:
                drop_from_high = (stock['price_eur'] - high_90d) / high_90d
                
                if drop_from_high <= -thresholds['min_drop_from_high']:
                    # Check for duplicate alerts
                    recent_alerts = db.get_recent_alerts(ticker, 
                                                        config['alerts']['duplicate_alert_days'])
                    
                    if not recent_alerts:
                        reason = (f"High volatility stock (beta {stock['beta']:.2f}) "
                                f"down {abs(drop_from_high)*100:.1f}% from 90-day high. ")
                        
                        if stock['pe_ratio'] > 0:
                            reason += f"P/E: {stock['pe_ratio']:.1f}."
                        
                        opportunities.append({
                            'ticker': ticker,
                            'name': stock['name'],
                            'sector': stock['sector'],
                            'country': stock['country'],
                            'price_eur': stock['price_eur'],
                            'high_90d': high_90d,
                            'beta': stock['beta'],
                            'volatility': stock['volatility'],
                            'pe_ratio': stock['pe_ratio'],
                            'market_cap_eur': stock['market_cap_eur'],
                            'drop_from_high': drop_from_high,
                            'reason': reason
                        })
    
    # Sort by drop magnitude
    opportunities.sort(key=lambda x: x['drop_from_high'])
    
    return opportunities


def save_opportunities(opportunities: List[Dict], strategy: str):
    """Save opportunities to JSON file for email alerts."""
    output_dir = Path(__file__).parent / 'output'
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = output_dir / f'{strategy}_opportunities_{timestamp}.json'
    
    with open(filename, 'w') as f:
        json.dump(opportunities, f, indent=2)
    
    print(f"Saved {len(opportunities)} opportunities to {filename}")


def record_alerts(db: StockDatabase, opportunities: List[Dict], strategy: str):
    """Record alerts in database."""
    for opp in opportunities:
        metrics = json.dumps({
            k: v for k, v in opp.items() 
            if k not in ['ticker', 'name', 'reason', 'price_eur']
        })
        
        alert_id = db.add_alert(
            ticker=opp['ticker'],
            strategy=strategy,
            price_eur=opp['price_eur'],
            reason=opp['reason'],
            metrics=metrics
        )


def format_market_cap(cap_eur: float) -> str:
    """Format market cap in human-readable format."""
    if cap_eur >= 1_000_000_000:
        return f"€{cap_eur/1_000_000_000:.1f}B"
    elif cap_eur >= 1_000_000:
        return f"€{cap_eur/1_000_000:.0f}M"
    else:
        return f"€{cap_eur:.0f}"


def main():
    print("=" * 60)
    print("Enhanced Stock Screener")
    print("=" * 60)
    
    start_time = time.time()
    
    # Load config
    print("\nLoading configuration...")
    config = load_config()
    
    # Connect to database
    print("Connecting to database...")
    db = StockDatabase()
    db.connect()
    
    # Check database has data
    stats = db.get_stats()
    if stats['total_stocks'] == 0:
        print("\nERROR: Database is empty. Run fetch_data_historical.py first.")
        db.close()
        return
    
    print(f"Database has {stats['total_stocks']} stocks")
    print(f"Latest data: {stats['latest_data_date']}")
    print(f"Total historical data points: {stats['total_data_points']:,}")
    
    # Screen for dividend opportunities
    print("\n" + "=" * 60)
    print("DIVIDEND SCREENING (Yield Expansion Detection)")
    print("=" * 60)
    
    dividend_start = time.time()
    dividend_opps = screen_dividend_opportunities(db, config)
    dividend_time = time.time() - dividend_start
    
    print(f"\n✓ Found {len(dividend_opps)} dividend opportunities")
    print(f"  Execution time: {dividend_time:.1f}s")
    
    # Screen for volatility opportunities
    print("\n" + "=" * 60)
    print("VOLATILITY SCREENING (90-Day Analysis)")
    print("=" * 60)
    
    volatility_start = time.time()
    volatility_opps = screen_volatility_opportunities(db, config)
    volatility_time = time.time() - volatility_start
    
    print(f"\n✓ Found {len(volatility_opps)} volatility opportunities")
    print(f"  Execution time: {volatility_time:.1f}s")
    
    # Record screening results
    db.add_screening_result('dividend', 400, len(dividend_opps), dividend_time)
    db.add_screening_result('volatility', 400, len(volatility_opps), volatility_time)
    
    # Record alerts
    if dividend_opps:
        print("\nRecording dividend alerts...")
        record_alerts(db, dividend_opps, 'dividend')
        save_opportunities(dividend_opps, 'dividend')
    
    if volatility_opps:
        print("Recording volatility alerts...")
        record_alerts(db, volatility_opps, 'volatility')
        save_opportunities(volatility_opps, 'volatility')
    
    # Summary
    total_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("SCREENING COMPLETE")
    print("=" * 60)
    print(f"Dividend opportunities: {len(dividend_opps)}")
    print(f"Volatility opportunities: {len(volatility_opps)}")
    print(f"Total opportunities: {len(dividend_opps) + len(volatility_opps)}")
    print(f"Total execution time: {total_time:.1f}s")
    print("=" * 60)
    
    # Show sample opportunities
    if dividend_opps:
        print("\n" + "━" * 60)
        print("TOP DIVIDEND OPPORTUNITIES (Yield Expansion)")
        print("━" * 60)
        for i, opp in enumerate(dividend_opps[:5], 1):
            print(f"\n{i}. {opp['name']} ({opp['ticker']}) - €{opp['price_eur']:.2f}")
            print(f"   {opp['country']} | {opp['sector']} | {format_market_cap(opp['market_cap_eur'])}")
            print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"   Current Yield: {opp['dividend_yield']*100:.1f}%")
            print(f"   Historical Implied Yield: {opp['historical_implied_yield']*100:.1f}%")
            print(f"   Yield Expansion: +{opp['yield_expansion_pp']*100:.2f} pp")
            print(f"   ")
            print(f"   Dividend: €{opp['dividend_per_share']:.2f}/share")
            print(f"   Current Price: €{opp['price_eur']:.2f}")
            print(f"   90-Day Avg: €{opp['price_90d_avg']:.2f}")
            print(f"   Discount: {opp['price_discount']*100:.1f}%")
            print(f"   ")
            print(f"   P/E Ratio: {opp['pe_ratio']:.1f}")
            print(f"   Payout Ratio: {opp['payout_ratio']*100:.0f}%")
    
    if volatility_opps:
        print("\n" + "━" * 60)
        print("TOP VOLATILITY OPPORTUNITIES (90-Day Dips)")
        print("━" * 60)
        for i, opp in enumerate(volatility_opps[:5], 1):
            print(f"\n{i}. {opp['name']} ({opp['ticker']}) - €{opp['price_eur']:.2f}")
            print(f"   {opp['country']} | {opp['sector']} | {format_market_cap(opp['market_cap_eur'])}")
            print(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"   Beta: {opp['beta']:.2f} | Volatility: {opp['volatility']:.2f}")
            print(f"   90-Day High: €{opp['high_90d']:.2f}")
            print(f"   Current: €{opp['price_eur']:.2f}")
            print(f"   Drop: {abs(opp['drop_from_high'])*100:.1f}%")
            if opp['pe_ratio'] > 0:
                print(f"   P/E Ratio: {opp['pe_ratio']:.1f}")
    
    db.close()


if __name__ == '__main__':
    main()
