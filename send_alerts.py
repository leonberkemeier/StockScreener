#!/usr/bin/env python3
"""
Daily automation script: Run screener and send email alerts for new opportunities.
Only sends alerts for opportunities that weren't detected yesterday.
"""

import sys
import json
from datetime import datetime
from common.email_alerts import EmailAlertSystem
from screener_enhanced import DividendScreener, VolatilityScreener


def load_opportunities_from_screener():
    """
    Run the screening logic and return opportunities.
    
    Returns:
        Tuple of (dividend_opportunities, volatility_opportunities)
    """
    print("=== Running Stock Screener ===\n")
    
    # Run dividend screening
    print("Running dividend screening...")
    dividend_screener = DividendScreener()
    dividend_opps = dividend_screener.screen()
    print(f"✅ Found {len(dividend_opps)} dividend opportunities\n")
    
    # Run volatility screening
    print("Running volatility screening...")
    volatility_screener = VolatilityScreener()
    volatility_opps = volatility_screener.screen()
    print(f"✅ Found {len(volatility_opps)} volatility opportunities\n")
    
    return dividend_opps, volatility_opps


def main():
    """Main execution: Run screener and send email alerts."""
    try:
        # Run screening
        dividend_opps, volatility_opps = load_opportunities_from_screener()
        
        # Initialize email system
        email_system = EmailAlertSystem()
        
        # Process and send alerts (only new opportunities)
        stats = email_system.process_and_send_alerts(
            dividend_opps,
            volatility_opps,
            lookback_days=1  # Only check yesterday
        )
        
        print("\n=== Summary ===")
        print(f"Total opportunities found: {stats['total_dividend'] + stats['total_volatility']}")
        print(f"New alerts sent: {stats['new_dividend'] + stats['new_volatility']}")
        print(f"  - Dividend: {stats['new_dividend']}")
        print(f"  - Volatility: {stats['new_volatility']}")
        
        if stats['new_dividend'] + stats['new_volatility'] == 0:
            print("\nℹ️  No email sent (no new opportunities)")
        
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
