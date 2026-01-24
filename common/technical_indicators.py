"""
Technical indicators for stock analysis.
Includes RSI, moving averages, 52-week high/low, and other indicators.
"""

from typing import List, Dict, Optional, Tuple
import numpy as np


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    """
    Calculate Relative Strength Index (RSI).
    
    Args:
        prices: List of prices (oldest to newest)
        period: RSI period (default: 14)
        
    Returns:
        RSI value (0-100) or None if insufficient data
    """
    if len(prices) < period + 1:
        return None
    
    # Calculate price changes
    deltas = np.diff(prices)
    
    # Separate gains and losses
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    
    # Calculate average gain and loss
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    
    if avg_loss == 0:
        return 100  # No losses = overbought
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_sma(prices: List[float], period: int) -> Optional[float]:
    """
    Calculate Simple Moving Average (SMA).
    
    Args:
        prices: List of prices (oldest to newest)
        period: Number of periods for average
        
    Returns:
        SMA value or None if insufficient data
    """
    if len(prices) < period:
        return None
    
    return np.mean(prices[-period:])


def calculate_ema(prices: List[float], period: int) -> Optional[float]:
    """
    Calculate Exponential Moving Average (EMA).
    
    Args:
        prices: List of prices (oldest to newest)
        period: Number of periods for average
        
    Returns:
        EMA value or None if insufficient data
    """
    if len(prices) < period:
        return None
    
    prices_array = np.array(prices)
    weights = np.exp(np.linspace(-1., 0., period))
    weights /= weights.sum()
    
    ema = np.convolve(prices_array, weights, mode='valid')[-1]
    return float(ema)


def calculate_moving_averages(prices: List[float]) -> Dict[str, Optional[float]]:
    """
    Calculate common moving averages (20, 50, 200 day).
    
    Args:
        prices: List of prices (oldest to newest)
        
    Returns:
        Dict with ma_20, ma_50, ma_200
    """
    return {
        'ma_20': calculate_sma(prices, 20),
        'ma_50': calculate_sma(prices, 50),
        'ma_200': calculate_sma(prices, 200)
    }


def detect_golden_cross(prices: List[float], lookback_days: int = 5) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Detect if golden cross occurred recently (50-day MA crosses above 200-day MA).
    
    Args:
        prices: List of prices (oldest to newest)
        lookback_days: Days to look back for crossover
        
    Returns:
        Tuple of (golden_cross_detected, ma_50, ma_200)
    """
    if len(prices) < 200:
        return False, None, None
    
    ma_50_current = calculate_sma(prices, 50)
    ma_200_current = calculate_sma(prices, 200)
    
    if not ma_50_current or not ma_200_current:
        return False, ma_50_current, ma_200_current
    
    # Check if currently in golden cross (50 above 200)
    if ma_50_current <= ma_200_current:
        return False, ma_50_current, ma_200_current
    
    # Check if crossover happened recently
    # Look back and see if 50-day was below 200-day
    for i in range(1, min(lookback_days + 1, len(prices) - 200)):
        ma_50_past = calculate_sma(prices[:-i], 50)
        ma_200_past = calculate_sma(prices[:-i], 200)
        
        if ma_50_past and ma_200_past and ma_50_past <= ma_200_past:
            return True, ma_50_current, ma_200_current
    
    return False, ma_50_current, ma_200_current


def detect_death_cross(prices: List[float], lookback_days: int = 5) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Detect if death cross occurred recently (50-day MA crosses below 200-day MA).
    
    Args:
        prices: List of prices (oldest to newest)
        lookback_days: Days to look back for crossover
        
    Returns:
        Tuple of (death_cross_detected, ma_50, ma_200)
    """
    if len(prices) < 200:
        return False, None, None
    
    ma_50_current = calculate_sma(prices, 50)
    ma_200_current = calculate_sma(prices, 200)
    
    if not ma_50_current or not ma_200_current:
        return False, ma_50_current, ma_200_current
    
    # Check if currently in death cross (50 below 200)
    if ma_50_current >= ma_200_current:
        return False, ma_50_current, ma_200_current
    
    # Check if crossover happened recently
    for i in range(1, min(lookback_days + 1, len(prices) - 200)):
        ma_50_past = calculate_sma(prices[:-i], 50)
        ma_200_past = calculate_sma(prices[:-i], 200)
        
        if ma_50_past and ma_200_past and ma_50_past >= ma_200_past:
            return True, ma_50_current, ma_200_current
    
    return False, ma_50_current, ma_200_current


def calculate_52_week_high_low(prices: List[float]) -> Dict[str, Optional[float]]:
    """
    Calculate 52-week (252 trading days) high and low.
    
    Args:
        prices: List of prices (oldest to newest)
        
    Returns:
        Dict with week_52_high, week_52_low
    """
    if len(prices) < 252:
        # Use available data if less than 252 days
        period = len(prices)
    else:
        period = 252
    
    recent_prices = prices[-period:]
    
    return {
        'week_52_high': max(recent_prices),
        'week_52_low': min(recent_prices)
    }


def calculate_volatility(prices: List[float], period: int = 30) -> Optional[float]:
    """
    Calculate historical volatility (standard deviation of returns).
    
    Args:
        prices: List of prices (oldest to newest)
        period: Number of days for calculation
        
    Returns:
        Annualized volatility or None if insufficient data
    """
    if len(prices) < period + 1:
        return None
    
    # Calculate daily returns
    returns = np.diff(prices[-period-1:]) / prices[-period-1:-1]
    
    # Standard deviation of returns
    volatility = np.std(returns)
    
    # Annualize (252 trading days)
    annualized_vol = volatility * np.sqrt(252)
    
    return float(annualized_vol)


def is_oversold(rsi: float, threshold: float = 30) -> bool:
    """Check if RSI indicates oversold condition."""
    return rsi < threshold


def is_overbought(rsi: float, threshold: float = 70) -> bool:
    """Check if RSI indicates overbought condition."""
    return rsi > threshold


def is_above_ma(current_price: float, ma: float) -> bool:
    """Check if current price is above moving average."""
    return current_price > ma


def is_near_52_week_low(current_price: float, week_52_low: float, threshold_pct: float = 0.05) -> bool:
    """
    Check if current price is near 52-week low.
    
    Args:
        current_price: Current stock price
        week_52_low: 52-week low price
        threshold_pct: % distance from low (default: 5%)
        
    Returns:
        True if within threshold of 52-week low
    """
    distance = (current_price - week_52_low) / week_52_low
    return distance <= threshold_pct


def calculate_all_indicators(prices: List[float], current_price: float) -> Dict:
    """
    Calculate all technical indicators for a stock.
    
    Args:
        prices: Historical prices (oldest to newest)
        current_price: Current price (should be last item in prices)
        
    Returns:
        Dict with all calculated indicators
    """
    indicators = {
        'rsi': calculate_rsi(prices, 14),
        'current_price': current_price
    }
    
    # Moving averages
    mas = calculate_moving_averages(prices)
    indicators.update(mas)
    
    # 52-week high/low
    week_52 = calculate_52_week_high_low(prices)
    indicators.update(week_52)
    
    # Golden/death cross detection
    golden_cross, _, _ = detect_golden_cross(prices)
    death_cross, _, _ = detect_death_cross(prices)
    indicators['golden_cross'] = golden_cross
    indicators['death_cross'] = death_cross
    
    # Volatility
    indicators['volatility_30d'] = calculate_volatility(prices, 30)
    
    # Derived flags
    if indicators['rsi']:
        indicators['is_oversold'] = is_oversold(indicators['rsi'])
        indicators['is_overbought'] = is_overbought(indicators['rsi'])
    else:
        indicators['is_oversold'] = None
        indicators['is_overbought'] = None
    
    if indicators['ma_50']:
        indicators['above_ma_50'] = is_above_ma(current_price, indicators['ma_50'])
    else:
        indicators['above_ma_50'] = None
    
    if indicators['ma_200']:
        indicators['above_ma_200'] = is_above_ma(current_price, indicators['ma_200'])
    else:
        indicators['above_ma_200'] = None
    
    if indicators['week_52_low']:
        indicators['near_52w_low'] = is_near_52_week_low(
            current_price, 
            indicators['week_52_low'], 
            threshold_pct=0.05
        )
    else:
        indicators['near_52w_low'] = None
    
    return indicators


if __name__ == '__main__':
    # Test with sample data
    import random
    
    # Generate sample price data (200 days)
    base_price = 100
    prices = []
    for i in range(200):
        base_price += random.uniform(-2, 2)
        prices.append(max(base_price, 50))  # Floor at 50
    
    current_price = prices[-1]
    
    print("Technical Indicators Test")
    print("=" * 60)
    print(f"Current Price: ${current_price:.2f}\n")
    
    indicators = calculate_all_indicators(prices, current_price)
    
    print(f"RSI (14-day): {indicators['rsi']:.2f}" if indicators['rsi'] else "RSI: N/A")
    print(f"20-day MA: ${indicators['ma_20']:.2f}" if indicators['ma_20'] else "20-day MA: N/A")
    print(f"50-day MA: ${indicators['ma_50']:.2f}" if indicators['ma_50'] else "50-day MA: N/A")
    print(f"200-day MA: ${indicators['ma_200']:.2f}" if indicators['ma_200'] else "200-day MA: N/A")
    print(f"\n52-Week High: ${indicators['week_52_high']:.2f}")
    print(f"52-Week Low: ${indicators['week_52_low']:.2f}")
    print(f"\nGolden Cross: {indicators['golden_cross']}")
    print(f"Death Cross: {indicators['death_cross']}")
    print(f"\nOversold: {indicators['is_oversold']}")
    print(f"Overbought: {indicators['is_overbought']}")
    print(f"Above 50-day MA: {indicators['above_ma_50']}")
    print(f"Near 52-week Low: {indicators['near_52w_low']}")
