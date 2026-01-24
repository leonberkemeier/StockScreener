#!/usr/bin/env python3
"""
Daily automation script: Run screener and send email alerts for new opportunities.
Only sends alerts for opportunities that weren't detected yesterday.
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime
from common.email_alerts import EmailAlertSystem
from common.database import StockDatabase
from screener_enhanced import (
    screen_dividend_opportunities,
    screen_volatility_opportunities,
    load_config
)
from screener_phase1 import (
    screen_52_week_low,
    screen_golden_cross
)


# Load .env file if it exists
def load_env_file():
    """Load environment variables from .env file."""
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())


load_env_file()


def load_opportunities_from_screener():
    """
    Run the screening logic and return opportunities.
    
    Returns:
        Dict with all strategy opportunities
    """
    print("=== Running Stock Screener ===\n")
    
    # Load config
    config = load_config()
    
    # Connect to database
    db = StockDatabase()
    db.connect()
    
    try:
        # Run dividend screening
        print("Running dividend screening...")
        dividend_opps = screen_dividend_opportunities(db, config)
        print(f"✅ Found {len(dividend_opps)} dividend opportunities\n")
        
        # Run volatility screening
        print("Running volatility screening...")
        volatility_opps = screen_volatility_opportunities(db, config)
        print(f"✅ Found {len(volatility_opps)} volatility opportunities\n")
        
        # Run 52-week low screening
        print("Running 52-week low screening...")
        week_52_low_opps = screen_52_week_low(db, config)
        print(f"✅ Found {len(week_52_low_opps)} at 52-week lows\n")
        
        # Run golden cross screening
        print("Running golden cross screening...")
        golden_cross_opps = screen_golden_cross(db, config)
        print(f"✅ Found {len(golden_cross_opps)} golden cross opportunities\n")
        
        return {
            'dividend': dividend_opps,
            'volatility': volatility_opps,
            '52_week_low': week_52_low_opps,
            'golden_cross': golden_cross_opps
        }
    
    finally:
        db.close()


def main():
    """Main execution: Run screener and send email alerts."""
    try:
        # Run screening
        all_opps = load_opportunities_from_screener()
        
        # Initialize email system
        email_system = EmailAlertSystem()
        
        # Process and send alerts for each strategy
        # Combine all opportunities for email
        all_opportunities_list = [
            *all_opps['dividend'],
            *all_opps['volatility'],
            *all_opps['52_week_low'],
            *all_opps['golden_cross']
        ]
        
        # Tag each opportunity with its strategy
        for opp in all_opps['dividend']:
            opp['strategy'] = 'dividend'
        for opp in all_opps['volatility']:
            opp['strategy'] = 'volatility'
        for opp in all_opps['52_week_low']:
            opp['strategy'] = '52_week_low'
        for opp in all_opps['golden_cross']:
            opp['strategy'] = 'golden_cross'
        
        # Process and send alerts (only new opportunities)
        stats = email_system.process_and_send_alerts(
            all_opps['dividend'],
            all_opps['volatility'],
            all_opps['52_week_low'],
            all_opps['golden_cross'],
            lookback_days=1  # Only check yesterday
        )
        
        print("\n=== Summary ===")
        print(f"Dividend: {len(all_opps['dividend'])} found, {stats['new_dividend']} new")
        print(f"Volatility: {len(all_opps['volatility'])} found, {stats['new_volatility']} new")
        print(f"52-Week Low: {len(all_opps['52_week_low'])} found, {stats['new_52_week_low']} new")
        print(f"Golden Cross: {len(all_opps['golden_cross'])} found, {stats['new_golden_cross']} new")
        
        total_found = sum(len(opps) for opps in all_opps.values())
        total_new = (stats['new_dividend'] + stats['new_volatility'] + 
                    stats['new_52_week_low'] + stats['new_golden_cross'])
        
        print(f"\nTotal: {total_found} opportunities, {total_new} new alerts sent")
        
        if total_new == 0:
            print("ℹ️  No email sent (no new opportunities)")
        
        return 0
        
    except ValueError as e:
        if "Email configuration missing" in str(e):
            print("❌ Email configuration missing!")
            print("\nPlease set the following environment variables:")
            print("  - SENDER_EMAIL: Your email address")
            print("  - SENDER_PASSWORD: Your email password/app password")
            print("  - RECIPIENT_EMAIL: Where to send alerts")
            print("  - SMTP_SERVER (optional, defaults to smtp.gmail.com)")
            print("  - SMTP_PORT (optional, defaults to 587)")
            print("\nExample:")
            print("  export SENDER_EMAIL='your-email@gmail.com'")
            print("  export SENDER_PASSWORD='your-app-password'")
            print("  export RECIPIENT_EMAIL='your-email@gmail.com'")
            return 1
        else:
            print(f"❌ Error: {e}")
            return 1
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
