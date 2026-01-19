#!/usr/bin/env python3
"""
Stock Screener - Identifies undervalued dividend and volatility opportunities.
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


def screen_dividend_opportunities(db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Screen for undervalued dividend opportunities.
    
    Criteria:
    - High dividend yield (>3%)
    - Undervalued (P/E < 15)
    - Recent price drop (>5%)
    - Sustainable payout (<80%)
    """
    thresholds = config['dividend']
    
    # Get top dividend stocks
    print("\nQuerying top 400 dividend stocks...")
    top_dividend = db.get_top_dividend_stocks(400)
    print(f"Found {len(top_dividend)} dividend-paying stocks")
    
    opportunities = []
    
    for stock in top_dividend:
        ticker = stock['ticker']
        
        # Check if meets opportunity criteria
        if (stock['dividend_yield'] >= thresholds['min_yield'] and
            stock['pe_ratio'] > 0 and 
            stock['pe_ratio'] <= thresholds['max_pe_ratio'] and
            stock['market_cap_eur'] >= thresholds['min_market_cap_eur']):
            
            # Check for recent price drop
            history = db.get_stock_history(ticker, days=7)
            if len(history) >= 2:
                current_price = history[0]['price_eur']
                old_price = history[-1]['price_eur']
                
                if old_price and old_price > 0:
                    price_change = (current_price - old_price) / old_price
                    
                    if price_change <= -thresholds['price_drop_threshold']:
                        # Check for duplicate alerts
                        recent_alerts = db.get_recent_alerts(ticker, 
                                                            config['alerts']['duplicate_alert_days'])
                        
                        if not recent_alerts:
                            reason = (f"High dividend yield ({stock['dividend_yield']*100:.1f}%) "
                                    f"with undervalued P/E ({stock['pe_ratio']:.1f}). "
                                    f"Price dropped {abs(price_change)*100:.1f}% in last 7 days.")
                            
                            opportunities.append({
                                'ticker': ticker,
                                'name': stock['name'],
                                'sector': stock['sector'],
                                'country': stock['country'],
                                'price_eur': stock['price_eur'],
                                'dividend_yield': stock['dividend_yield'],
                                'pe_ratio': stock['pe_ratio'],
                                'market_cap_eur': stock['market_cap_eur'],
                                'price_change_7d': price_change,
                                'reason': reason
                            })
    
    return opportunities


def screen_volatility_opportunities(db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Screen for high volatility opportunities.
    
    Criteria:
    - High beta or volatility
    - Undervalued (P/E < 25)
    - Recent dip from 30-day high
    """
    thresholds = config['volatility']
    
    # Get top volatility stocks
    print("\nQuerying top 400 volatility stocks...")
    top_volatility = db.get_top_volatility_stocks(400)
    print(f"Found {len(top_volatility)} high-volatility stocks")
    
    opportunities = []
    
    for stock in top_volatility:
        ticker = stock['ticker']
        
        # Check if meets opportunity criteria
        if (stock['market_cap_eur'] >= thresholds['min_market_cap_eur'] and
            (stock['beta'] >= thresholds['min_beta'] or 
             stock['volatility'] >= thresholds['min_volatility'])):
            
            # Check P/E if available
            if stock['pe_ratio'] > 0 and stock['pe_ratio'] > thresholds['max_pe_ratio']:
                continue
            
            # Check for recent dip from high
            if stock['year_high_eur'] and stock['price_eur']:
                drop_from_high = (stock['price_eur'] - stock['year_high_eur']) / stock['year_high_eur']
                
                if drop_from_high <= -thresholds['min_drop_from_high']:
                    # Check for duplicate alerts
                    recent_alerts = db.get_recent_alerts(ticker, 
                                                        config['alerts']['duplicate_alert_days'])
                    
                    if not recent_alerts:
                        reason = (f"High volatility stock (beta {stock['beta']:.2f}) "
                                f"down {abs(drop_from_high)*100:.1f}% from 52-week high. ")
                        
                        if stock['pe_ratio'] > 0:
                            reason += f"P/E ratio: {stock['pe_ratio']:.1f}."
                        
                        opportunities.append({
                            'ticker': ticker,
                            'name': stock['name'],
                            'sector': stock['sector'],
                            'country': stock['country'],
                            'price_eur': stock['price_eur'],
                            'beta': stock['beta'],
                            'volatility': stock['volatility'],
                            'pe_ratio': stock['pe_ratio'],
                            'market_cap_eur': stock['market_cap_eur'],
                            'drop_from_high': drop_from_high,
                            'year_high_eur': stock['year_high_eur'],
                            'reason': reason
                        })
    
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


def main():
    print("=" * 60)
    print("Stock Screener")
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
        print("\nERROR: Database is empty. Run fetch_data.py first.")
        db.close()
        return
    
    print(f"Database has {stats['total_stocks']} stocks")
    print(f"Latest data: {stats['latest_data_date']}")
    
    # Screen for dividend opportunities
    print("\n" + "=" * 60)
    print("DIVIDEND SCREENING")
    print("=" * 60)
    
    dividend_start = time.time()
    dividend_opps = screen_dividend_opportunities(db, config)
    dividend_time = time.time() - dividend_start
    
    print(f"\n✓ Found {len(dividend_opps)} dividend opportunities")
    print(f"  Execution time: {dividend_time:.1f}s")
    
    # Screen for volatility opportunities
    print("\n" + "=" * 60)
    print("VOLATILITY SCREENING")
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
        print("\nTop 3 Dividend Opportunities:")
        for i, opp in enumerate(dividend_opps[:3], 1):
            print(f"\n{i}. {opp['name']} ({opp['ticker']}) - €{opp['price_eur']:.2f}")
            print(f"   Yield: {opp['dividend_yield']*100:.1f}% | P/E: {opp['pe_ratio']:.1f}")
            print(f"   {opp['reason']}")
    
    if volatility_opps:
        print("\nTop 3 Volatility Opportunities:")
        for i, opp in enumerate(volatility_opps[:3], 1):
            print(f"\n{i}. {opp['name']} ({opp['ticker']}) - €{opp['price_eur']:.2f}")
            print(f"   Beta: {opp['beta']:.2f} | Drop: {abs(opp['drop_from_high'])*100:.1f}%")
            print(f"   {opp['reason']}")
    
    db.close()


if __name__ == '__main__':
    main()
