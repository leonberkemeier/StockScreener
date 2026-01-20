#!/usr/bin/env python3
"""
Database module for stock screener.
Handles SQLite operations for storing stock data and screening results.
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from contextlib import contextmanager


@contextmanager
def get_db_connection(db_path: str = "stock_screener.db"):
    """Context manager for database connections.
    
    Usage:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(...)
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


class StockDatabase:
    """Manages SQLite database for stock screening."""
    
    def __init__(self, db_path: str = "stock_screener.db"):
        """Initialize database connection."""
        self.db_path = Path(db_path)
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable dict-like access
        self.cursor = self.conn.cursor()
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def init_schema(self):
        """Create database schema."""
        
        # Table 1: stocks - Master list of all tracked stocks
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker TEXT PRIMARY KEY,
                name TEXT,
                sector TEXT,
                country TEXT,
                currency TEXT,
                exchange TEXT,
                added_date TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Table 2: stock_data - Daily stock metrics
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                price_eur REAL,
                market_cap_eur REAL,
                dividend_yield REAL,
                payout_ratio REAL,
                beta REAL,
                pe_ratio REAL,
                pb_ratio REAL,
                volatility REAL,
                volume INTEGER,
                day_high_eur REAL,
                day_low_eur REAL,
                year_high_eur REAL,
                year_low_eur REAL,
                fetch_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker),
                UNIQUE(ticker, date)
            )
        """)
        
        # Table 3: screening_results - Results from each screening run
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS screening_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TEXT NOT NULL,
                strategy TEXT NOT NULL,
                total_stocks_screened INTEGER,
                opportunities_found INTEGER,
                execution_time_seconds REAL
            )
        """)
        
        # Table 4: alerts - Stocks that triggered alerts
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                strategy TEXT NOT NULL,
                alert_date TEXT NOT NULL,
                alert_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                price_eur REAL,
                reason TEXT,
                metrics TEXT,
                sent_email BOOLEAN DEFAULT 0,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            )
        """)
        
        # Create indexes for performance
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_data_ticker_date 
            ON stock_data(ticker, date DESC)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_ticker_date 
            ON alerts(ticker, alert_date)
        """)
        
        self.conn.commit()
        print("✓ Database schema initialized")
    
    def add_stock(self, ticker: str, name: str, sector: str, 
                  country: str, currency: str, exchange: str = ""):
        """Add a stock to the master list."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO stocks 
                (ticker, name, sector, country, currency, exchange)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, name, sector, country, currency, exchange))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding stock {ticker}: {e}")
    
    def add_stock_data(self, data: Dict):
        """Add daily stock data."""
        try:
            self.cursor.execute("""
                INSERT OR REPLACE INTO stock_data 
                (ticker, date, price_eur, market_cap_eur, dividend_yield,
                 payout_ratio, beta, pe_ratio, pb_ratio, volatility,
                 volume, day_high_eur, day_low_eur, year_high_eur, year_low_eur)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['ticker'],
                data['date'],
                data.get('price_eur'),
                data.get('market_cap_eur'),
                data.get('dividend_yield', 0),
                data.get('payout_ratio', 0),
                data.get('beta', 0),
                data.get('pe_ratio', 0),
                data.get('pb_ratio', 0),
                data.get('volatility', 0),
                data.get('volume', 0),
                data.get('day_high_eur'),
                data.get('day_low_eur'),
                data.get('year_high_eur'),
                data.get('year_low_eur')
            ))
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding stock data for {data.get('ticker')}: {e}")
    
    def add_bulk_stock_data(self, data_list: List[Dict]):
        """Add multiple stock data entries efficiently."""
        for data in data_list:
            self.add_stock_data(data)
    
    def get_latest_data(self, ticker: str) -> Optional[Dict]:
        """Get most recent data for a stock."""
        self.cursor.execute("""
            SELECT * FROM stock_data 
            WHERE ticker = ?
            ORDER BY date DESC 
            LIMIT 1
        """, (ticker,))
        row = self.cursor.fetchone()
        return dict(row) if row else None
    
    def get_top_dividend_stocks(self, limit: int = 400) -> List[Dict]:
        """Get top stocks by dividend yield."""
        self.cursor.execute("""
            SELECT sd.*, s.name, s.sector, s.country
            FROM stock_data sd
            JOIN stocks s ON sd.ticker = s.ticker
            WHERE sd.date = (
                SELECT MAX(date) FROM stock_data WHERE ticker = sd.ticker
            )
            AND sd.dividend_yield > 0
            AND sd.market_cap_eur > 100000000
            ORDER BY sd.dividend_yield DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_top_volatility_stocks(self, limit: int = 400) -> List[Dict]:
        """Get top stocks by volatility score."""
        self.cursor.execute("""
            SELECT sd.*, s.name, s.sector, s.country,
                   (sd.beta + sd.volatility) as volatility_score
            FROM stock_data sd
            JOIN stocks s ON sd.ticker = s.ticker
            WHERE sd.date = (
                SELECT MAX(date) FROM stock_data WHERE ticker = sd.ticker
            )
            AND sd.market_cap_eur > 100000000
            AND (sd.beta > 1.2 OR s.sector IN 
                 ('Technology', 'Healthcare', 'Biotechnology', 
                  'Energy', 'Basic Materials', 'Consumer Cyclical'))
            ORDER BY volatility_score DESC
            LIMIT ?
        """, (limit,))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def add_alert(self, ticker: str, strategy: str, price_eur: float,
                  reason: str, metrics: str):
        """Record an alert."""
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            self.cursor.execute("""
                INSERT INTO alerts 
                (ticker, strategy, alert_date, price_eur, reason, metrics)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ticker, strategy, today, price_eur, reason, metrics))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error adding alert for {ticker}: {e}")
            return None
    
    def mark_alert_sent(self, alert_id: int):
        """Mark an alert as email sent."""
        self.cursor.execute("""
            UPDATE alerts SET sent_email = 1 WHERE id = ?
        """, (alert_id,))
        self.conn.commit()
    
    def get_recent_alerts(self, ticker: str, days: int = 7) -> List[Dict]:
        """Check if ticker had recent alerts (avoid duplicates)."""
        self.cursor.execute("""
            SELECT * FROM alerts 
            WHERE ticker = ? 
            AND date(alert_date) >= date('now', '-' || ? || ' days')
            ORDER BY alert_date DESC
        """, (ticker, days))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def add_screening_result(self, strategy: str, total_screened: int,
                            opportunities: int, exec_time: float):
        """Record screening run results."""
        timestamp = datetime.now().isoformat()
        self.cursor.execute("""
            INSERT INTO screening_results 
            (run_timestamp, strategy, total_stocks_screened, 
             opportunities_found, execution_time_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, (timestamp, strategy, total_screened, opportunities, exec_time))
        self.conn.commit()
    
    def get_stock_history(self, ticker: str, days: int = 30) -> List[Dict]:
        """Get historical data for a stock."""
        self.cursor.execute("""
            SELECT * FROM stock_data 
            WHERE ticker = ?
            AND date >= date('now', '-' || ? || ' days')
            ORDER BY date DESC
        """, (ticker, days))
        return [dict(row) for row in self.cursor.fetchall()]
    
    def get_stats(self) -> Dict:
        """Get database statistics."""
        stats = {}
        
        # Total stocks
        self.cursor.execute("SELECT COUNT(*) FROM stocks")
        stats['total_stocks'] = self.cursor.fetchone()[0]
        
        # Latest data date
        self.cursor.execute("SELECT MAX(date) FROM stock_data")
        stats['latest_data_date'] = self.cursor.fetchone()[0]
        
        # Total data points
        self.cursor.execute("SELECT COUNT(*) FROM stock_data")
        stats['total_data_points'] = self.cursor.fetchone()[0]
        
        # Total alerts
        self.cursor.execute("SELECT COUNT(*) FROM alerts")
        stats['total_alerts'] = self.cursor.fetchone()[0]
        
        return stats


def main():
    """Initialize database schema."""
    db = StockDatabase()
    db.connect()
    db.init_schema()
    
    print("\n" + "=" * 60)
    print("Database initialized: stock_screener.db")
    print("=" * 60)
    
    stats = db.get_stats()
    print(f"\nDatabase Stats:")
    print(f"  Total stocks: {stats['total_stocks']}")
    print(f"  Latest data: {stats['latest_data_date']}")
    print(f"  Total data points: {stats['total_data_points']}")
    print(f"  Total alerts: {stats['total_alerts']}")
    
    db.close()


if __name__ == '__main__':
    main()
