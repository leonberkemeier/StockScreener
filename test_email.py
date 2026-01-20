#!/usr/bin/env python3
"""
Test email system with mock opportunity data.
"""

import sys
import os
from pathlib import Path

# Load .env file
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

from common.email_alerts import EmailAlertSystem

# Create mock opportunity data
mock_dividend_opportunity = {
    'ticker': 'TEST.NS',
    'name': 'Test Company Limited',
    'sector': 'Technology',
    'country': 'India',
    'price_eur': 42.50,
    'price_90d_avg': 50.00,
    'price_discount': 0.15,  # 15% discount
    'dividend_yield': 0.045,  # 4.5%
    'dividend_per_share': 1.91,
    'historical_implied_yield': 0.038,  # 3.8%
    'yield_expansion_pp': 0.007,  # +0.7 pp
    'pe_ratio': 18.5,
    'payout_ratio': 0.65,
    'market_cap_eur': 5000000000,  # 5B
}

mock_volatility_opportunity = {
    'ticker': 'VOLT.DE',
    'name': 'Volatility AG',
    'sector': 'Technology',
    'country': 'Germany',
    'price_eur': 85.00,
    'high_90d': 110.00,
    'drop_from_high': -0.227,  # -22.7%
    'beta': 1.8,
    'volatility': 0.35,
    'pe_ratio': 22.0,
    'market_cap_eur': 3500000000,  # 3.5B
}

def main():
    """Send test email."""
    try:
        print("=== Email Test ===\n")
        
        # Initialize email system
        email_system = EmailAlertSystem()
        
        print(f"SMTP Server: {email_system.smtp_server}:{email_system.smtp_port}")
        print(f"Sender: {email_system.sender_email}")
        print(f"Recipient: {email_system.recipient_email}\n")
        
        # Send test email with mock data
        print("Sending test email with mock opportunity data...")
        email_system.send_email(
            [mock_dividend_opportunity],
            [mock_volatility_opportunity]
        )
        
        print("\n✅ Test email sent successfully!")
        print("Check your inbox (and spam folder) for the email.")
        
        return 0
        
    except ValueError as e:
        if "Email configuration missing" in str(e):
            print("❌ Email configuration missing!")
            print("\nPlease check your .env file has:")
            print("  - SENDER_EMAIL")
            print("  - SENDER_PASSWORD")
            print("  - RECIPIENT_EMAIL")
            return 1
        else:
            print(f"❌ Error: {e}")
            return 1
    
    except Exception as e:
        print(f"❌ Failed to send test email: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
