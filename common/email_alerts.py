"""
Email alerting system for stock screening opportunities.
Only sends alerts for NEW opportunities that weren't detected in recent runs.
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import List, Dict, Any
from common.database import get_db_connection


class EmailAlertSystem:
    """Handles email alerts for stock screening opportunities."""
    
    def __init__(self):
        """Initialize email system with SMTP configuration from environment."""
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.sender_email = os.getenv('SENDER_EMAIL')
        self.sender_password = os.getenv('SENDER_PASSWORD')
        self.recipient_email = os.getenv('RECIPIENT_EMAIL')
        self.cc_email = os.getenv('CC_EMAIL', '')  # Optional CC recipient
        
        if not all([self.sender_email, self.sender_password, self.recipient_email]):
            raise ValueError(
                "Email configuration missing. Set SENDER_EMAIL, SENDER_PASSWORD, "
                "and RECIPIENT_EMAIL environment variables."
            )
    
    def get_recent_alerts(self, lookback_days: int = 7) -> List[Dict[str, Any]]:
        """
        Get alerts sent in the last N days.
        
        Args:
            lookback_days: Number of days to look back
            
        Returns:
            List of alert records
        """
        cutoff_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ticker, strategy, alert_date, metrics
                FROM alerts
                WHERE alert_date >= ?
                ORDER BY alert_date DESC
            """, (cutoff_date,))
            
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def filter_new_opportunities(
        self, 
        opportunities: List[Dict[str, Any]], 
        strategy_type: str,
        lookback_days: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Filter opportunities to only include NEW ones not alerted recently.
        
        Args:
            opportunities: List of screening opportunities
            strategy_type: 'dividend' or 'volatility'
            lookback_days: How many days to check for duplicates
            
        Returns:
            List of new opportunities only
        """
        recent_alerts = self.get_recent_alerts(lookback_days)
        
        # Create set of (ticker, strategy) tuples for recent alerts
        recent_alert_keys = {
            (alert['ticker'], alert['strategy']) 
            for alert in recent_alerts
        }
        
        # Filter to only opportunities not in recent alerts
        new_opportunities = [
            opp for opp in opportunities
            if (opp['ticker'], strategy_type) not in recent_alert_keys
        ]
        
        return new_opportunities
    
    def insert_alert(
        self, 
        ticker: str, 
        strategy_type: str, 
        details: Dict[str, Any]
    ) -> None:
        """
        Insert alert record into database.
        
        Args:
            ticker: Stock ticker
            strategy_type: 'dividend' or 'volatility'
            details: Opportunity details dict
        """
        import json
        
        # Convert numpy/boolean types to JSON-serializable types
        def make_json_serializable(obj):
            if isinstance(obj, dict):
                return {k: make_json_serializable(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [make_json_serializable(item) for item in obj]
            elif isinstance(obj, bool):
                return bool(obj)
            elif isinstance(obj, (int, float)):
                return float(obj) if isinstance(obj, float) else int(obj)
            elif obj is None:
                return None
            else:
                return str(obj)
        
        serializable_details = make_json_serializable(details)
        
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO alerts 
                (ticker, strategy, alert_date, price_eur, reason, metrics)
                VALUES (?, ?, DATE('now'), ?, ?, ?)
            """, (
                ticker, 
                strategy_type,
                details.get('price_eur', 0),
                f"{strategy_type} opportunity detected",
                json.dumps(serializable_details)
            ))
            conn.commit()
    
    def format_dividend_opportunity(self, opp: Dict[str, Any]) -> str:
        """Format a dividend opportunity as HTML."""
        # Format volatility for display
        volatility = opp.get('volatility', 0)
        if isinstance(volatility, (int, float)) and volatility > 0:
            volatility_str = f"{volatility*100:.2f}%" if volatility < 1 else f"{volatility:.2f}%"
            volatility_display = f"<p style=\"margin: 5px 0;\"><strong>Volatility:</strong> {volatility_str}</p>"
        else:
            volatility_display = ""
        
        return f"""
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px;">
            <h3 style="margin-top: 0; color: #2c5aa0;">
                {opp['ticker']} - {opp.get('name', 'N/A')}
            </h3>
            <p><strong>Sector:</strong> {opp.get('sector', 'N/A')} | 
               <strong>Country:</strong> {opp.get('country', 'N/A')}</p>
            
            <div style="background: #f5f5f5; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Current Price:</strong> €{opp['price_eur']:.2f}</p>
                <p style="margin: 5px 0;"><strong>90-Day Avg Price:</strong> €{opp['price_90d_avg']:.2f}</p>
                <p style="margin: 5px 0; color: #c0392b;">
                    <strong>Price Discount:</strong> {opp['price_discount']*100:.1f}%
                </p>
            </div>
            
            <div style="background: #e8f5e9; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Current Yield:</strong> {opp['dividend_yield']*100:.2f}%</p>
                <p style="margin: 5px 0;"><strong>Historical Yield:</strong> {opp['historical_implied_yield']*100:.2f}%</p>
                <p style="margin: 5px 0; color: #27ae60;">
                    <strong>Yield Expansion:</strong> +{opp['yield_expansion_pp']*100:.2f} pp
                </p>
                <p style="margin: 5px 0;"><strong>Dividend/Share:</strong> €{opp['dividend_per_share']:.4f}</p>
            </div>
            
            <p><strong>P/E Ratio:</strong> {opp.get('pe_ratio', 'N/A')}</p>
            <p><strong>Payout Ratio:</strong> {opp.get('payout_ratio', 'N/A')}</p>
            {volatility_display}
            <p><strong>Market Cap:</strong> €{opp.get('market_cap_eur', 0)/1e9:.1f}B</p>
        </div>
        """
    
    def format_volatility_opportunity(self, opp: Dict[str, Any]) -> str:
        """Format a volatility opportunity as HTML."""
        # Format volatility as percentage if it's a decimal
        volatility = opp.get('volatility', 0)
        if isinstance(volatility, (int, float)) and volatility > 0:
            volatility_str = f"{volatility*100:.2f}%" if volatility < 1 else f"{volatility:.2f}%"
        else:
            volatility_str = str(volatility)
        
        # Format beta
        beta = opp.get('beta', 'N/A')
        if isinstance(beta, (int, float)):
            beta_str = f"{beta:.2f}"
        else:
            beta_str = str(beta)
        
        return f"""
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px;">
            <h3 style="margin-top: 0; color: #8e44ad;">
                {opp['ticker']} - {opp.get('name', 'N/A')}
            </h3>
            <p><strong>Sector:</strong> {opp.get('sector', 'N/A')} | 
               <strong>Country:</strong> {opp.get('country', 'N/A')}</p>
            
            <div style="background: #f5f5f5; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Current Price:</strong> €{opp['price_eur']:.2f}</p>
                <p style="margin: 5px 0;"><strong>90-Day High:</strong> €{opp['high_90d']:.2f}</p>
                <p style="margin: 5px 0; color: #c0392b;">
                    <strong>Drop from High:</strong> {abs(opp['drop_from_high'])*100:.1f}%
                </p>
            </div>
            
            <div style="background: #fef5e7; padding: 10px; margin: 10px 0; border-left: 4px solid #e74c3c;">
                <p style="margin: 5px 0;"><strong>📊 Volatility (Annualized):</strong> <span style="color: #e74c3c; font-weight: bold; font-size: 1.1em;">{volatility_str}</span></p>
                <p style="margin: 5px 0;"><strong>Beta (Market Risk):</strong> <span style="font-weight: bold;">{beta_str}</span></p>
            </div>
            
            <p><strong>P/E Ratio:</strong> {opp.get('pe_ratio', 'N/A')}</p>
            <p><strong>Market Cap:</strong> €{opp.get('market_cap_eur', 0)/1e9:.1f}B</p>
        </div>
        """
    
    def format_52_week_low_opportunity(self, opp: Dict[str, Any]) -> str:
        """Format a 52-week low opportunity as HTML."""
        return f"""
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px;">
            <h3 style="margin-top: 0; color: #c0392b;">
                {opp['ticker']} - {opp.get('name', 'N/A')}
            </h3>
            <p><strong>Sector:</strong> {opp.get('sector', 'N/A')} | 
               <strong>Country:</strong> {opp.get('country', 'N/A')}</p>
            
            <div style="background: #ffebee; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Current Price:</strong> €{opp['price_eur']:.2f}</p>
                <p style="margin: 5px 0;"><strong>52-Week Low:</strong> €{opp['week_52_low']:.2f}</p>
                <p style="margin: 5px 0;"><strong>52-Week High:</strong> €{opp['week_52_high']:.2f}</p>
                <p style="margin: 5px 0; color: #c0392b;">
                    <strong>Distance from Low:</strong> {opp['distance_from_low_pct']*100:.1f}%
                </p>
            </div>
            
            <div style="background: #e8f5e9; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Dividend Yield:</strong> {opp['dividend_yield']*100:.2f}%</p>
                <p style="margin: 5px 0;"><strong>P/E Ratio:</strong> {opp.get('pe_ratio', 'N/A')}</p>
                {f'<p style="margin: 5px 0;"><strong>RSI:</strong> {opp["rsi"]:.1f} (Oversold)</p>' if opp.get('is_oversold') else ''}
            </div>
            
            <p><strong>Market Cap:</strong> €{opp.get('market_cap_eur', 0)/1e9:.1f}B</p>
        </div>
        """
    
    def format_golden_cross_opportunity(self, opp: Dict[str, Any]) -> str:
        """Format a golden cross opportunity as HTML."""
        return f"""
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px;">
            <h3 style="margin-top: 0; color: #f39c12;">
                {opp['ticker']} - {opp.get('name', 'N/A')}
            </h3>
            <p><strong>Sector:</strong> {opp.get('sector', 'N/A')} | 
               <strong>Country:</strong> {opp.get('country', 'N/A')}</p>
            
            <div style="background: #fff9e6; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Current Price:</strong> €{opp['price_eur']:.2f}</p>
                <p style="margin: 5px 0; color: #27ae60;">
                    <strong>50-Day MA:</strong> €{opp['ma_50']:.2f} ✓
                </p>
                <p style="margin: 5px 0;">
                    <strong>200-Day MA:</strong> €{opp['ma_200']:.2f}
                </p>
                <p style="margin: 5px 0; color: #f39c12;">
                    <strong>Golden Cross Detected!</strong> Bullish momentum signal.
                </p>
            </div>
            
            <div style="background: #f5f5f5; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>P/E Ratio:</strong> {opp.get('pe_ratio', 'N/A')}</p>
                {f'<p style="margin: 5px 0;"><strong>Dividend Yield:</strong> {opp["dividend_yield"]*100:.2f}%</p>' if opp.get('dividend_yield', 0) > 0 else ''}
            </div>
            
            <p><strong>Market Cap:</strong> €{opp.get('market_cap_eur', 0)/1e9:.1f}B</p>
        </div>
        """
    
    def format_top_opportunity_card(self, opp: Dict[str, Any], strategy: str) -> str:
        """Format top opportunity as a detailed card (matching new opportunities style)."""
        ticker = opp.get('ticker', 'N/A')
        name = opp.get('name', 'N/A')
        sector = opp.get('sector', 'N/A')
        country = opp.get('country', 'N/A')
        price = opp.get('price_eur', 0)
        alert_count = opp.get('alert_count', 0)
        
        # Format volatility if available
        volatility = opp.get('volatility', 0)
        if isinstance(volatility, (int, float)) and volatility > 0:
            volatility_str = f"{volatility*100:.2f}%" if volatility < 1 else f"{volatility:.2f}%"
            volatility_display = f"<p style=\"margin: 5px 0;\"><strong>Volatility:</strong> {volatility_str}</p>"
        else:
            volatility_display = ""
        
        # Format beta if available
        beta = opp.get('beta', 0)
        if isinstance(beta, (int, float)) and beta > 0:
            beta_str = f"{beta:.2f}"
        else:
            beta_str = "N/A"
        
        # Choose color based on strategy
        color = "#2c5aa0" if strategy == 'dividend' else "#8e44ad"
        emoji = "💰" if strategy == 'dividend' else "⚡"
        
        # Format dividend yield if available
        div_yield = opp.get('dividend_yield', 0)
        if isinstance(div_yield, (int, float)):
            if div_yield > 1:
                div_yield = div_yield / 100
            div_yield_str = f"{div_yield*100:.2f}%"
        else:
            div_yield_str = "N/A"
        
        # Format PE ratio
        pe_ratio = opp.get('pe_ratio', 0)
        if isinstance(pe_ratio, str):
            try:
                pe_ratio = float(pe_ratio)
            except (ValueError, TypeError):
                pe_ratio = 0
        pe_str = f"{pe_ratio:.1f}" if pe_ratio > 0 else "N/A"
        
        # Format market cap
        market_cap = opp.get('market_cap_eur', 0)
        if market_cap:
            market_cap_str = f"€{market_cap/1e9:.1f}B"
        else:
            market_cap_str = "N/A"
        
        html = f"""
        <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; background: #fafafa;">
            <h3 style="margin-top: 0; color: {color};">
                {emoji} {ticker} - {name}
            </h3>
            <p><strong>Sector:</strong> {sector} | <strong>Country:</strong> {country}</p>
            
            <div style="background: #fff; padding: 10px; margin: 10px 0; border-left: 3px solid {color};">
                <p style="margin: 5px 0;"><strong>Current Price:</strong> €{price:.2f}</p>
                <p style="margin: 5px 0;"><strong>Alerted:</strong> {alert_count}x (proven winner)</p>
            </div>
            
            <div style="background: #f5f5f5; padding: 10px; margin: 10px 0;">
                <p style="margin: 5px 0;"><strong>Dividend Yield:</strong> {div_yield_str}</p>
                <p style="margin: 5px 0;"><strong>P/E Ratio:</strong> {pe_str}</p>
                {volatility_display}
        """
        
        if strategy == 'volatility':
            html += f"""
                <p style="margin: 5px 0;"><strong>Beta (Market Risk):</strong> {beta_str}</p>
            </div>
            """
        else:
            html += """
            </div>
            """
        
        html += f"""
            <p><strong>Market Cap:</strong> {market_cap_str}</p>
        </div>
        """
        
        return html
    
    def create_enhanced_email_html(
        self,
        top_dividend: List[Dict] = None,
        top_volatility: List[Dict] = None,
        new_dividend: List[Dict[str, Any]] = None,
        new_volatility: List[Dict[str, Any]] = None,
        new_52w_low: List[Dict[str, Any]] = None,
        new_golden_cross: List[Dict[str, Any]] = None
    ) -> str:
        """
        Create enhanced HTML email with top 20 best + new opportunities.
        
        Args:
            top_dividend: Top 20 best dividend opportunities (from alerts)
            top_volatility: Top 20 best volatility opportunities (from alerts)
            new_dividend: New dividend opportunities
            new_volatility: New volatility opportunities
            new_52w_low: New 52-week low opportunities
            new_golden_cross: New golden cross opportunities
            
        Returns:
            HTML string
        """
        top_dividend = top_dividend or []
        top_volatility = top_volatility or []
        new_dividend = new_dividend or []
        new_volatility = new_volatility or []
        new_52w_low = new_52w_low or []
        new_golden_cross = new_golden_cross or []
        
        total_new = len(new_dividend) + len(new_volatility) + len(new_52w_low) + len(new_golden_cross)
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                h1 {{ color: #2c5aa0; margin-bottom: 5px; }}
                h2 {{ color: #333; border-bottom: 3px solid #2c5aa0; padding-bottom: 10px; margin-top: 25px; }}
                h3 {{ color: #555; margin-top: 15px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th {{ background: #f0f0f0; padding: 10px; text-align: left; border-bottom: 2px solid #2c5aa0; }}
                td {{ padding: 8px; border-bottom: 1px solid #eee; }}
                .section-new {{ background: #e8f4f8; padding: 15px; margin: 20px 0; border-left: 4px solid #2c5aa0; border-radius: 3px; }}
                .section-top {{ background: #f9f9f9; padding: 15px; margin: 20px 0; }}
                .timestamp {{ color: #888; font-size: 0.9em; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 15px; }}
            </style>
        </head>
        <body>
            <h1>📈 Stock Screener Daily Report</h1>
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d')}</p>
            <p><strong>Total New Opportunities:</strong> {total_new}</p>
        """
        
        # SECTION 1: TOP 20 BEST (Detailed Cards)
        if top_dividend or top_volatility:
            html += """
            <h2>🏆 TOP 20 BEST OPPORTUNITIES (Proven Winners)</h2>
            """
            
            if top_dividend:
                html += """
                <h3>💰 Top Dividend Opportunities</h3>
                """
                for opp in top_dividend[:20]:
                    html += self.format_top_opportunity_card(opp, 'dividend')
            
            if top_volatility:
                html += """
                <h3>⚡ Top Volatility Opportunities</h3>
                """
                for opp in top_volatility[:20]:
                    html += self.format_top_opportunity_card(opp, 'volatility')
        
        # SECTION 2: NEW OPPORTUNITIES (Detailed)
        if total_new > 0:
            html += f"""
            <div class="section-new">
                <h2>🆕 NEW OPPORTUNITIES TODAY ({total_new} new)</h2>
            </div>
            """
            
            if new_dividend:
                html += f"""
                <h3>💰 New Dividend Opportunities ({len(new_dividend)})</h3>
                """
                for opp in new_dividend:
                    html += self.format_dividend_opportunity(opp)
            
            if new_volatility:
                html += f"""
                <h3>⚡ New Volatility Opportunities ({len(new_volatility)})</h3>
                """
                for opp in new_volatility:
                    html += self.format_volatility_opportunity(opp)
            
            if new_52w_low:
                html += f"""
                <h3>📉 New 52-Week Low Opportunities ({len(new_52w_low)})</h3>
                <p style="font-style: italic; color: #666;">Quality stocks at yearly lows - contrarian value plays</p>
                """
                for opp in new_52w_low:
                    html += self.format_52_week_low_opportunity(opp)
            
            if new_golden_cross:
                html += f"""
                <h3>🌟 New Golden Cross Opportunities ({len(new_golden_cross)})</h3>
                <p style="font-style: italic; color: #666;">50-day MA crossed above 200-day MA - bullish momentum</p>
                """
                for opp in new_golden_cross:
                    html += self.format_golden_cross_opportunity(opp)
        else:
            html += """
            <div class="section-new">
                <p style="color: #7f8c8d; font-style: italic;">
                    ✓ No new opportunities today. All current opportunities were already alerted.
                </p>
            </div>
            """
        
        html += f"""
        <div class="timestamp">
            <p><em>Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}</em></p>
            <p><em>Stock Screener v2 - Automated Daily Report</em></p>
        </div>
        
        </body>
        </html>
        """
        
        return html
    
    def create_email_html(
        self, 
        dividend_opps: List[Dict[str, Any]], 
        volatility_opps: List[Dict[str, Any]],
        week_52_low_opps: List[Dict[str, Any]] = None,
        golden_cross_opps: List[Dict[str, Any]] = None
    ) -> str:
        """
        Create HTML email body with opportunities.
        
        Args:
            dividend_opps: List of dividend opportunities
            volatility_opps: List of volatility opportunities
            week_52_low_opps: List of 52-week low opportunities
            golden_cross_opps: List of golden cross opportunities
            
        Returns:
            HTML string
        """
        week_52_low_opps = week_52_low_opps or []
        golden_cross_opps = golden_cross_opps or []
        
        total_opps = len(dividend_opps) + len(volatility_opps) + len(week_52_low_opps) + len(golden_cross_opps)
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                h2 {{ color: #333; border-bottom: 2px solid #333; padding-bottom: 10px; }}
            </style>
        </head>
        <body>
            <h1 style="color: #2c5aa0;">📈 Stock Screener: New Opportunities</h1>
            <p><em>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}</em></p>
            <p><strong>Total: {total_opps} opportunit{'y' if total_opps == 1 else 'ies'}</strong></p>
        """
        
        if dividend_opps:
            html += f"""
            <h2>💰 Dividend Opportunities ({len(dividend_opps)})</h2>
            """
            for opp in dividend_opps:
                html += self.format_dividend_opportunity(opp)
        
        if volatility_opps:
            html += f"""
            <h2>⚡ Volatility Opportunities ({len(volatility_opps)})</h2>
            """
            for opp in volatility_opps:
                html += self.format_volatility_opportunity(opp)
        
        if week_52_low_opps:
            html += f"""
            <h2>📉 52-Week Low Opportunities ({len(week_52_low_opps)})</h2>
            <p style="font-style: italic; color: #666;">Quality stocks at yearly lows - contrarian value plays</p>
            """
            for opp in week_52_low_opps:
                html += self.format_52_week_low_opportunity(opp)
        
        if golden_cross_opps:
            html += f"""
            <h2>🌟 Golden Cross Opportunities ({len(golden_cross_opps)})</h2>
            <p style="font-style: italic; color: #666;">50-day MA crossed above 200-day MA - bullish momentum</p>
            """
            for opp in golden_cross_opps:
                html += self.format_golden_cross_opportunity(opp)
        
        if total_opps == 0:
            html += """
            <p style="color: #7f8c8d; font-style: italic;">
                No new opportunities today. All current opportunities were already alerted.
            </p>
            """
        
        html += """
        </body>
        </html>
        """
        
        return html
    
    def send_email_with_top_20(
        self,
        top_dividend_opps: List[Dict] = None,
        top_volatility_opps: List[Dict] = None,
        new_dividend_opportunities: List[Dict[str, Any]] = None,
        new_volatility_opportunities: List[Dict[str, Any]] = None,
        new_week_52_low_opportunities: List[Dict[str, Any]] = None,
        new_golden_cross_opportunities: List[Dict[str, Any]] = None
    ) -> None:
        """
        Send enhanced email with top 20 best + new opportunities.
        
        Args:
            top_dividend_opps: Top 20 best dividend opportunities
            top_volatility_opps: Top 20 best volatility opportunities
            new_dividend_opportunities: New dividend opportunities
            new_volatility_opportunities: New volatility opportunities
            new_week_52_low_opportunities: New 52-week low opportunities
            new_golden_cross_opportunities: New golden cross opportunities
        """
        top_dividend_opps = top_dividend_opps or []
        top_volatility_opps = top_volatility_opps or []
        new_dividend_opportunities = new_dividend_opportunities or []
        new_volatility_opportunities = new_volatility_opportunities or []
        new_week_52_low_opportunities = new_week_52_low_opportunities or []
        new_golden_cross_opportunities = new_golden_cross_opportunities or []
        
        total_new = (len(new_dividend_opportunities) + len(new_volatility_opportunities) + 
                     len(new_week_52_low_opportunities) + len(new_golden_cross_opportunities))
        
        # Create email
        msg = MIMEMultipart('alternative')
        subject = f"Stock Screener Report: {total_new} New Opportunit{'y' if total_new == 1 else 'ies'}"
        if not total_new:
            subject = "Stock Screener Report: No New Opportunities"
        msg['Subject'] = subject
        msg['From'] = self.sender_email
        msg['To'] = self.recipient_email
        
        # Add CC if configured
        if self.cc_email:
            msg['Cc'] = self.cc_email
        
        # Create enhanced HTML body
        html_body = self.create_enhanced_email_html(
            top_dividend=top_dividend_opps,
            top_volatility=top_volatility_opps,
            new_dividend=new_dividend_opportunities,
            new_volatility=new_volatility_opportunities,
            new_52w_low=new_week_52_low_opportunities,
            new_golden_cross=new_golden_cross_opportunities
        )
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            print(f"✅ Email sent successfully to {self.recipient_email}")
            if self.cc_email:
                print(f"   CC: {self.cc_email}")
            print(f"   New opportunities: {total_new}")
            print(f"   - {len(new_dividend_opportunities)} dividend")
            print(f"   - {len(new_volatility_opportunities)} volatility")
            print(f"   - {len(new_week_52_low_opportunities)} 52-week low")
            print(f"   - {len(new_golden_cross_opportunities)} golden cross")
            
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            raise
    
    def send_email(
        self, 
        dividend_opportunities: List[Dict[str, Any]], 
        volatility_opportunities: List[Dict[str, Any]],
        week_52_low_opportunities: List[Dict[str, Any]] = None,
        golden_cross_opportunities: List[Dict[str, Any]] = None
    ) -> None:
        """
        Send email alert with new opportunities.
        
        Args:
            dividend_opportunities: List of dividend opportunities
            volatility_opportunities: List of volatility opportunities
            week_52_low_opportunities: List of 52-week low opportunities
            golden_cross_opportunities: List of golden cross opportunities
        """
        week_52_low_opportunities = week_52_low_opportunities or []
        golden_cross_opportunities = golden_cross_opportunities or []
        
        total_opps = (len(dividend_opportunities) + len(volatility_opportunities) + 
                     len(week_52_low_opportunities) + len(golden_cross_opportunities))
        
        if total_opps == 0:
            print("No new opportunities to email.")
            return
        
        # Create email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Stock Screener Alert: {total_opps} New Opportunit{'y' if total_opps == 1 else 'ies'}"
        msg['From'] = self.sender_email
        msg['To'] = self.recipient_email
        
        # Add CC if configured
        if self.cc_email:
            msg['Cc'] = self.cc_email
        
        # Create HTML body
        html_body = self.create_email_html(
            dividend_opportunities, 
            volatility_opportunities,
            week_52_low_opportunities,
            golden_cross_opportunities
        )
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            
            print(f"✅ Email sent successfully to {self.recipient_email}")
            if self.cc_email:
                print(f"   CC: {self.cc_email}")
            print(f"   - {len(dividend_opportunities)} dividend")
            print(f"   - {len(volatility_opportunities)} volatility")
            print(f"   - {len(week_52_low_opportunities)} 52-week low")
            print(f"   - {len(golden_cross_opportunities)} golden cross")
            
        except Exception as e:
            print(f"❌ Failed to send email: {e}")
            raise
    
    def process_and_send_alerts(
        self,
        dividend_opportunities: List[Dict[str, Any]],
        volatility_opportunities: List[Dict[str, Any]],
        week_52_low_opportunities: List[Dict[str, Any]] = None,
        golden_cross_opportunities: List[Dict[str, Any]] = None,
        lookback_days: int = 1
    ) -> Dict[str, int]:
        """
        Main method: Filter new opportunities, send email, and record alerts.
        
        Args:
            dividend_opportunities: All dividend opportunities from screening
            volatility_opportunities: All volatility opportunities from screening
            week_52_low_opportunities: All 52-week low opportunities
            golden_cross_opportunities: All golden cross opportunities
            lookback_days: Days to check for duplicate alerts (default: 1 = yesterday only)
            
        Returns:
            Dict with counts of new opportunities sent
        """
        week_52_low_opportunities = week_52_low_opportunities or []
        golden_cross_opportunities = golden_cross_opportunities or []
        
        # Filter to only NEW opportunities
        new_dividend = self.filter_new_opportunities(
            dividend_opportunities, 
            'dividend', 
            lookback_days
        )
        new_volatility = self.filter_new_opportunities(
            volatility_opportunities, 
            'volatility', 
            lookback_days
        )
        new_52w_low = self.filter_new_opportunities(
            week_52_low_opportunities,
            '52_week_low',
            lookback_days
        )
        new_golden_cross = self.filter_new_opportunities(
            golden_cross_opportunities,
            'golden_cross',
            lookback_days
        )
        
        print(f"\n=== Email Alert Processing ===")
        print(f"Dividend: {len(dividend_opportunities)} total, {len(new_dividend)} new")
        print(f"Volatility: {len(volatility_opportunities)} total, {len(new_volatility)} new")
        print(f"52-Week Low: {len(week_52_low_opportunities)} total, {len(new_52w_low)} new")
        print(f"Golden Cross: {len(golden_cross_opportunities)} total, {len(new_golden_cross)} new")
        
        # Send email if there are new opportunities
        total_new = len(new_dividend) + len(new_volatility) + len(new_52w_low) + len(new_golden_cross)
        
        if total_new > 0:
            self.send_email(
                new_dividend, 
                new_volatility,
                new_52w_low,
                new_golden_cross
            )
            
            # Record alerts in database
            for opp in new_dividend:
                self.insert_alert(opp['ticker'], 'dividend', opp)
            
            for opp in new_volatility:
                self.insert_alert(opp['ticker'], 'volatility', opp)
            
            for opp in new_52w_low:
                self.insert_alert(opp['ticker'], '52_week_low', opp)
            
            for opp in new_golden_cross:
                self.insert_alert(opp['ticker'], 'golden_cross', opp)
            
            print(f"✅ Recorded {total_new} alerts in database")
        else:
            print("ℹ️  No new opportunities to email (all were already alerted)")
        
        return {
            'new_dividend': len(new_dividend),
            'new_volatility': len(new_volatility),
            'new_52_week_low': len(new_52w_low),
            'new_golden_cross': len(new_golden_cross),
            'total_dividend': len(dividend_opportunities),
            'total_volatility': len(volatility_opportunities),
            'total_52_week_low': len(week_52_low_opportunities),
            'total_golden_cross': len(golden_cross_opportunities)
        }
    
    def process_and_send_alerts_with_top_20(
        self,
        dividend_opportunities: List[Dict[str, Any]],
        volatility_opportunities: List[Dict[str, Any]],
        week_52_low_opportunities: List[Dict[str, Any]] = None,
        golden_cross_opportunities: List[Dict[str, Any]] = None,
        top_dividend_from_db: List[Dict] = None,
        top_volatility_from_db: List[Dict] = None,
        lookback_days: int = 1
    ) -> Dict[str, int]:
        """
        Main method: Filter new opportunities, send enhanced email with top 20 + new, and record alerts.
        
        Args:
            dividend_opportunities: All dividend opportunities from screening
            volatility_opportunities: All volatility opportunities from screening
            week_52_low_opportunities: All 52-week low opportunities
            golden_cross_opportunities: All golden cross opportunities
            top_dividend_from_db: Top 20 best dividend opportunities from database
            top_volatility_from_db: Top 20 best volatility opportunities from database
            lookback_days: Days to check for duplicate alerts (default: 1 = yesterday only)
            
        Returns:
            Dict with counts of new opportunities sent
        """
        week_52_low_opportunities = week_52_low_opportunities or []
        golden_cross_opportunities = golden_cross_opportunities or []
        top_dividend_from_db = top_dividend_from_db or []
        top_volatility_from_db = top_volatility_from_db or []
        
        # Filter to only NEW opportunities
        new_dividend = self.filter_new_opportunities(
            dividend_opportunities, 
            'dividend', 
            lookback_days
        )
        new_volatility = self.filter_new_opportunities(
            volatility_opportunities, 
            'volatility', 
            lookback_days
        )
        new_52w_low = self.filter_new_opportunities(
            week_52_low_opportunities,
            '52_week_low',
            lookback_days
        )
        new_golden_cross = self.filter_new_opportunities(
            golden_cross_opportunities,
            'golden_cross',
            lookback_days
        )
        
        print(f"\n=== Email Alert Processing (Enhanced with Top 20) ===")
        print(f"Dividend: {len(dividend_opportunities)} total, {len(new_dividend)} new")
        print(f"Volatility: {len(volatility_opportunities)} total, {len(new_volatility)} new")
        print(f"52-Week Low: {len(week_52_low_opportunities)} total, {len(new_52w_low)} new")
        print(f"Golden Cross: {len(golden_cross_opportunities)} total, {len(new_golden_cross)} new")
        print(f"Top 20 dividend from database: {len(top_dividend_from_db)}")
        print(f"Top 20 volatility from database: {len(top_volatility_from_db)}")
        
        # Send enhanced email with top 20 + new
        total_new = len(new_dividend) + len(new_volatility) + len(new_52w_low) + len(new_golden_cross)
        
        self.send_email_with_top_20(
            top_dividend_opps=top_dividend_from_db,
            top_volatility_opps=top_volatility_from_db,
            new_dividend_opportunities=new_dividend,
            new_volatility_opportunities=new_volatility,
            new_week_52_low_opportunities=new_52w_low,
            new_golden_cross_opportunities=new_golden_cross
        )
        
        if total_new > 0:
            # Record alerts in database
            for opp in new_dividend:
                self.insert_alert(opp['ticker'], 'dividend', opp)
            
            for opp in new_volatility:
                self.insert_alert(opp['ticker'], 'volatility', opp)
            
            for opp in new_52w_low:
                self.insert_alert(opp['ticker'], '52_week_low', opp)
            
            for opp in new_golden_cross:
                self.insert_alert(opp['ticker'], 'golden_cross', opp)
            
            print(f"✅ Recorded {total_new} alerts in database")
        else:
            print("ℹ️  No new opportunities to email (all were already alerted)")
        
        return {
            'new_dividend': len(new_dividend),
            'new_volatility': len(new_volatility),
            'new_52_week_low': len(new_52w_low),
            'new_golden_cross': len(new_golden_cross),
            'total_dividend': len(dividend_opportunities),
            'total_volatility': len(volatility_opportunities),
            'total_52_week_low': len(week_52_low_opportunities),
            'total_golden_cross': len(golden_cross_opportunities)
        }

