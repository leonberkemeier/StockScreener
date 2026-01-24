#!/usr/bin/env python3
"""
Phase 1 Screening Strategies
- 52-Week Low + Strong Fundamentals
- Golden Cross Detection
- Technical overlays for existing strategies
"""

import sys
from pathlib import Path
from typing import List, Dict

sys.path.insert(0, str(Path(__file__).parent))
from common.database import StockDatabase
from common.technical_indicators import (
    calculate_all_indicators,
    is_near_52_week_low,
    detect_golden_cross
)


def get_price_history(db: StockDatabase, ticker: str, days: int = 252) -> List[float]:
    """Get historical prices for technical analysis."""
    history = db.get_stock_history(ticker, days=days)
    prices = [h['price_eur'] for h in reversed(history) if h['price_eur']]
    return prices


def screen_52_week_low(db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Screen for quality stocks hitting 52-week lows.
    
    Strategy: Buy quality companies when they're out of favor.
    """
    thresholds = config['52_week_low']
    
    print("\n52-Week Low Screening...")
    
    # Get all stocks with sufficient data
    cursor = db.cursor
    cursor.execute("""
        SELECT sd.*, s.name, s.sector, s.country
        FROM stock_data sd
        JOIN stocks s ON sd.ticker = s.ticker
        WHERE sd.date = (
            SELECT MAX(date) FROM stock_data WHERE ticker = sd.ticker
        )
        AND sd.market_cap_eur >= ?
        AND sd.pe_ratio > 0
        AND sd.pe_ratio <= ?
        AND sd.dividend_yield >= ?
    """, (
        thresholds['min_market_cap_eur'],
        thresholds['max_pe_ratio'],
        thresholds['min_dividend_yield']
    ))
    
    candidates = [dict(row) for row in cursor.fetchall()]
    print(f"  Found {len(candidates)} stocks meeting basic criteria")
    
    opportunities = []
    
    for stock in candidates:
        ticker = stock['ticker']
        current_price = stock['price_eur']
        
        if not current_price:
            continue
        
        # Get price history
        prices = get_price_history(db, ticker, days=252)
        
        if len(prices) < 100:  # Need at least 100 days
            continue
        
        # Calculate technical indicators
        indicators = calculate_all_indicators(prices, current_price)
        
        if not indicators['week_52_low']:
            continue
        
        # Check if near 52-week low
        distance_from_low = (current_price - indicators['week_52_low']) / indicators['week_52_low']
        
        if distance_from_low <= thresholds['max_distance_from_low_pct']:
            # Check for duplicate alerts
            recent_alerts = db.get_recent_alerts(ticker, config['alerts']['duplicate_alert_days'])
            
            if not any(a['strategy'] == '52_week_low' for a in recent_alerts):
                # Normalize dividend yield
                dividend_yield = stock['dividend_yield']
                if dividend_yield > 1:
                    dividend_yield = dividend_yield / 100
                
                reason = (f"Trading at 52-week low (€{indicators['week_52_low']:.2f}). "
                         f"Current: €{current_price:.2f} ({distance_from_low*100:.1f}% above low). "
                         f"Strong fundamentals: P/E {stock['pe_ratio']:.1f}, "
                         f"Yield {dividend_yield*100:.1f}%.")
                
                if indicators['rsi']:
                    reason += f" RSI: {indicators['rsi']:.1f} (oversold)." if indicators['is_oversold'] else f" RSI: {indicators['rsi']:.1f}."
                
                opportunities.append({
                    'ticker': ticker,
                    'name': stock['name'],
                    'sector': stock['sector'],
                    'country': stock['country'],
                    'price_eur': current_price,
                    'week_52_low': indicators['week_52_low'],
                    'week_52_high': indicators['week_52_high'],
                    'distance_from_low_pct': distance_from_low,
                    'dividend_yield': dividend_yield,
                    'pe_ratio': stock['pe_ratio'],
                    'market_cap_eur': stock['market_cap_eur'],
                    'rsi': indicators['rsi'],
                    'is_oversold': indicators['is_oversold'],
                    'reason': reason
                })
    
    opportunities.sort(key=lambda x: x['distance_from_low_pct'])
    print(f"  Found {len(opportunities)} opportunities at 52-week lows\n")
    
    return opportunities


def screen_golden_cross(db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Screen for stocks with golden cross (50-day MA crosses above 200-day MA).
    
    Strategy: Momentum following with fundamental filters.
    """
    thresholds = config['golden_cross']
    
    if not thresholds.get('enabled', True):
        print("\nGolden Cross screening disabled in config")
        return []
    
    print("\nGolden Cross Screening...")
    
    # Get all stocks with sufficient data
    cursor = db.cursor
    cursor.execute("""
        SELECT sd.*, s.name, s.sector, s.country
        FROM stock_data sd
        JOIN stocks s ON sd.ticker = s.ticker
        WHERE sd.date = (
            SELECT MAX(date) FROM stock_data WHERE ticker = sd.ticker
        )
        AND sd.market_cap_eur >= ?
    """, (thresholds['min_market_cap_eur'],))
    
    candidates = [dict(row) for row in cursor.fetchall()]
    print(f"  Found {len(candidates)} stocks meeting basic criteria")
    
    opportunities = []
    
    for stock in candidates:
        ticker = stock['ticker']
        current_price = stock['price_eur']
        
        if not current_price:
            continue
        
        # Need P/E check
        if stock['pe_ratio'] > 0 and stock['pe_ratio'] > thresholds['max_pe_ratio']:
            continue
        
        # Get price history (need 200+ days for 200-day MA)
        prices = get_price_history(db, ticker, days=252)
        
        if len(prices) < 200:
            continue
        
        # Detect golden cross
        golden_cross, ma_50, ma_200 = detect_golden_cross(prices, thresholds['lookback_days'])
        
        if golden_cross:
            # Check dividend yield if specified
            dividend_yield = stock['dividend_yield']
            if dividend_yield > 1:
                dividend_yield = dividend_yield / 100
            
            if thresholds.get('min_dividend_yield', 0) > 0:
                if dividend_yield < thresholds['min_dividend_yield']:
                    continue
            
            # Check for duplicate alerts
            recent_alerts = db.get_recent_alerts(ticker, config['alerts']['duplicate_alert_days'])
            
            if not any(a['strategy'] == 'golden_cross' for a in recent_alerts):
                reason = (f"Golden Cross detected! 50-day MA (€{ma_50:.2f}) "
                         f"crossed above 200-day MA (€{ma_200:.2f}). "
                         f"Strong bullish momentum signal. ")
                
                if stock['pe_ratio'] > 0:
                    reason += f"P/E: {stock['pe_ratio']:.1f}. "
                
                if dividend_yield > 0:
                    reason += f"Yield: {dividend_yield*100:.1f}%."
                
                opportunities.append({
                    'ticker': ticker,
                    'name': stock['name'],
                    'sector': stock['sector'],
                    'country': stock['country'],
                    'price_eur': current_price,
                    'ma_50': ma_50,
                    'ma_200': ma_200,
                    'dividend_yield': dividend_yield,
                    'pe_ratio': stock['pe_ratio'],
                    'market_cap_eur': stock['market_cap_eur'],
                    'reason': reason
                })
    
    print(f"  Found {len(opportunities)} golden cross opportunities\n")
    
    return opportunities


def apply_technical_filters(opportunities: List[Dict], db: StockDatabase, config: Dict) -> List[Dict]:
    """
    Apply technical filters to existing opportunities.
    Enhances dividend/volatility strategies with RSI and MA confirmation.
    """
    tech_config = config.get('technical_filters', {})
    
    if not tech_config.get('use_rsi', False) and not tech_config.get('use_moving_averages', False):
        return opportunities  # No filters enabled
    
    filtered = []
    
    for opp in opportunities:
        ticker = opp['ticker']
        current_price = opp.get('price_eur', 0)
        
        if not current_price:
            filtered.append(opp)
            continue
        
        # Get price history
        prices = get_price_history(db, ticker, days=252)
        
        if len(prices) < 50:
            filtered.append(opp)  # Not enough data, keep original
            continue
        
        indicators = calculate_all_indicators(prices, current_price)
        
        # Apply RSI filter if enabled
        if tech_config.get('use_rsi', False):
            if indicators['rsi']:
                # For dividend/value strategies: prefer oversold (RSI < threshold)
                if indicators['rsi'] > tech_config.get('rsi_threshold', 40):
                    continue  # Filter out if not oversold
                
                opp['rsi'] = indicators['rsi']
                opp['is_oversold'] = indicators['is_oversold']
        
        # Apply MA filter if enabled
        if tech_config.get('use_moving_averages', False):
            if tech_config.get('require_above_50ma', False):
                if not indicators['above_ma_50']:
                    continue  # Filter out if below 50-day MA
            
            opp['ma_50'] = indicators['ma_50']
            opp['ma_200'] = indicators['ma_200']
        
        filtered.append(opp)
    
    return filtered


if __name__ == '__main__':
    import json
    
    # Load config
    config_file = Path(__file__).parent / 'config' / 'screening_thresholds.json'
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Connect to database
    db = StockDatabase()
    db.connect()
    
    try:
        print("=" * 80)
        print("PHASE 1 SCREENING STRATEGIES")
        print("=" * 80)
        
        # Run 52-week low screener
        low_52w_opps = screen_52_week_low(db, config)
        
        # Run golden cross screener
        golden_cross_opps = screen_golden_cross(db, config)
        
        print("\n" + "=" * 80)
        print("RESULTS SUMMARY")
        print("=" * 80)
        print(f"52-Week Low Opportunities: {len(low_52w_opps)}")
        print(f"Golden Cross Opportunities: {len(golden_cross_opps)}")
        
        if low_52w_opps:
            print("\nTop 5 at 52-Week Lows:")
            for opp in low_52w_opps[:5]:
                print(f"  {opp['ticker']:10} {opp['name'][:30]:30} "
                      f"€{opp['price_eur']:.2f} "
                      f"({opp['distance_from_low_pct']*100:.1f}% above low)")
        
        if golden_cross_opps:
            print("\nGolden Cross Stocks:")
            for opp in golden_cross_opps[:5]:
                print(f"  {opp['ticker']:10} {opp['name'][:30]:30} "
                      f"€{opp['price_eur']:.2f}")
        
    finally:
        db.close()
