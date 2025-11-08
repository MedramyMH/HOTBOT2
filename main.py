"""
Optimized Trading Signal Generator - Next Candle Predictions
Generates accurate signals for M1, M5, M15 timeframes
"""

import logging
import time
import schedule
import threading
from datetime import datetime,timedelta
from signal_generator import SignalGenerator
from database import PriceDatabase
import json
import requests
from typing import List, Dict

class TradingSignalBot:
    def __init__(self, config_path="config.json"):
        self.config = self.load_config(config_path)
        self.db = PriceDatabase()
        self.signal_generator = SignalGenerator(self.db)
        self.setup_logging()
        
        # Only M1, M5, M15 for binary options
        self.timeframes = [1, 5, 15]
        self.assets = self.config.get('assets', [
            'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'USDCAD',
            'AUDUSD', 'NZDUSD', 'EURGBP', 'EURJPY', 'GBPJPY'
        ])
        
        self.signals_sent = 0
        self.time_offset = 1  # ⏰ Offset in hours
        self.session_start = self.local_now()
        self.verification_threads = []  # Track verification threads


    def local_now(self):
        """Return current local time with timezone correction"""
        return datetime.now() + timedelta(hours=self.time_offset)
        
    def load_config(self, config_path):
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.warning(f"Config file not found: {config_path}, using defaults")
            return {
                "assets": ["EURUSD", "GBPUSD", "USDJPY", "USDCAD", "AUDUSD"],
                "min_confidence": 0.8,
                "telegram": {
                    "enabled": True,
                    "token": "8432186447:AAHStxiGWnqeLAk9XmeCS-ExwEuNSUsXWqg",
                    "chat_id": "1454104544"
                }
            }
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('signal_bot.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def check_data_availability(self) -> Dict:
        """Check if enough data is available for analysis"""
        stats = self.db.get_database_stats()
        
        if stats['total_records'] < 100:
            self.logger.warning(
                f"⚠️ Low data count: {stats['total_records']} records. "
                "Need more historical data for accurate signals."
            )
            return {'ready': False, 'reason': 'insufficient_data'}
        
        return {'ready': True, 'records': stats['total_records']}
    
    def generate_signals(self) -> List:
        """Generate trading signals for all assets and timeframes"""
        self.logger.info("🔍 Scanning market for trading opportunities...")
        
        # Check data availability
        data_check = self.check_data_availability()
        if not data_check.get('ready', False):
            self.logger.warning(f"⏳ Waiting for more data... ({data_check.get('reason')})")
            return []
        
        signals = []
        min_confidence = self.config.get('min_confidence', 0.7)
        
        for asset in self.assets:
            for timeframe in self.timeframes:
                try:
                    signal = self.signal_generator.analyze_asset(asset, timeframe)
                    
                    if signal and signal.confidence >= min_confidence:
                        signals.append(signal)
                        
                except Exception as e:
                    self.logger.error(f"Error analyzing {asset} M{timeframe}: {e}")
                    continue
                
                # Small delay to avoid overwhelming the system
                time.sleep(0.1)
        
        # Sort by confidence (highest first)
        signals.sort(key=lambda x: x.confidence, reverse=True)
        
        return signals
    
    def format_telegram_message(self, signal) -> str:
        """Format signal for Telegram with better readability"""
        
        # Emoji for direction
        direction_emoji = "📈 CALL" if signal.direction == "BUY" else "📉 PUT"
        
        # Confidence bar
        confidence_pct = signal.confidence * 100
        bars = int(confidence_pct / 10)
        confidence_bar = "█" * bars + "▒" * (10 - bars)
        
        # Timeframe expiry
        expiry_time = datetime.fromisoformat(signal.expiry_time)
        expiry_str = expiry_time.strftime('%H:%M:%S')
        
        message = f"""
🎯 *TRADING SIGNAL*

*Asset:* {signal.asset}
*Direction:* {direction_emoji}
*Timeframe:* M{signal.timeframe} ⚠️ 

💪 *Strength:* {signal.strength:.2f}/1.0
📊 *Confidence:* {confidence_pct:.1f}%
{confidence_bar}

💵 *Entry Price:* {signal.current_price:.5f}
🎯 *Target:* {signal.target_price:.5f}
⏰ *Expiry:* {expiry_str}
"""
# 📋 *Key Indicators:*
# • RSI: {signal.indicators['rsi']:.1f}
# • MACD: {signal.indicators['macd']:.5f}
# • Trend: {signal.indicators['trend']} ({signal.indicators['trend_strength']:.2f})

# ✅ *Reasons:*
# {chr(10).join(['• ' + r for r in signal.indicators['reasons'][:4]])}

# 🔔 Score: BUY {signal.indicators['buy_score']:.1f} | SELL {signal.indicators['sell_score']:.1f}

# ⚠️ *Trade Duration:* {signal.timeframe} minute(s)

        return message.strip()
    
    def send_telegram_signal(self, signal) -> bool:
        """Send signal to Telegram"""
        try:
            telegram_config = self.config.get('telegram', {})
            
            if not telegram_config.get('enabled', False):
                self.logger.debug("Telegram notifications disabled")
                return False
            
            token = telegram_config.get('token')
            chat_id = telegram_config.get('chat_id')
            
            if not token or not chat_id:
                self.logger.error("Telegram token or chat_id missing")
                return False
            
            message = self.format_telegram_message(signal)
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            
            response = requests.post(url, data=data, timeout=10)
            
            if response.status_code == 200:
                self.signals_sent += 1
                self.logger.info(f"✅ Signal sent to Telegram: {signal.asset} M{signal.timeframe}")
                return True
            else:
                self.logger.error(f"Telegram API error: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error sending Telegram message: {e}")
            return False
    
    def schedule_signal_verification(self, signal_id: str, signal):
        """
        Schedule automatic verification of a signal in a background thread.
        This will wait until expiry + 1 minute and then verify the result.
        """
        try:
            # Create a background thread for this specific signal
            verification_thread = threading.Thread(
                target=self.db.verify_signal_result,
                args=(
                    signal_id,
                    signal.asset,
                    signal.timeframe,
                    signal.direction,
                    signal.current_price,
                    signal.expiry_time
                ),
                daemon=True,
                name=f"Verify-{signal.asset}-M{signal.timeframe}-{signal_id[:8]}"
            )
            
            verification_thread.start()
            self.verification_threads.append(verification_thread)
            
            self.logger.info(
                f"🕐 Verification scheduled for signal {signal_id} "
                f"({signal.asset} M{signal.timeframe})"
            )
            
        except Exception as e:
            self.logger.error(f"Error scheduling verification: {e}")
    
    def process_signals(self, signals: List):
        """Process and send signals"""
        if not signals:
            self.logger.info("🔭 No high-confidence signals at this time")
            return
        
        self.logger.info(f"🎯 Found {len(signals)} trading signal(s)")
        
        # Limit to top 3 signals per scan to avoid spam
        top_signals = signals[:3]
        
        for idx, signal in enumerate(top_signals, 1):
            self.logger.info(
                f"📊 Signal {idx}/{len(top_signals)}: "
                f"{signal.asset} M{signal.timeframe} | {signal.direction} | "
                f"Confidence: {signal.confidence:.1%} | "
                f"Entry: {signal.current_price:.5f}"
            )
            
            # Send to Telegram
            telegram_sent = self.send_telegram_signal(signal)
            
            # Save signal to database
            if telegram_sent:
                signal_id = self.db.save_signal(signal)
                
                # Schedule automatic verification
                if signal_id:
                    self.schedule_signal_verification(signal_id, signal)
            
            # Delay between messages
            if idx < len(top_signals):
                time.sleep(2)
    
    def run_analysis(self):
        """Run market analysis and generate signals"""
        try:
            self.logger.info("="*70)
            self.logger.info(f"🔄 Analysis started at {self.local_now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Generate signals
            signals = self.generate_signals()
            
            # Process and send
            self.process_signals(signals)
            
            # Show session stats
            uptime = self.local_now() - self.session_start
            self.logger.info(
                f"📊 Session: {self.signals_sent} signals sent | "
                f"Uptime: {str(uptime).split('.')[0]}"
            )
            self.logger.info("="*70)
            
        except Exception as e:
            self.logger.error(f"Error in analysis: {e}", exc_info=True)
    
    def send_startup_message(self):
        """Send bot startup notification"""
        try:
            telegram_config = self.config.get('telegram', {})
            if not telegram_config.get('enabled', False):
                return
            
            token = telegram_config.get('token')
            chat_id = telegram_config.get('chat_id')
            
            if not token or not chat_id:
                return
            
            db_stats = self.db.get_database_stats()
            
            message = f"""
🤖 *SIGNAL BOT STARTED*

✅ Status: Active
📊 Database: {db_stats['total_records']:,} records
📈 Assets: {len(self.assets)}
⏱️ Timeframes: M1, M5, M15
🎯 Min Confidence: {self.config.get('min_confidence', 0.7):.0%}

🔔 Bot is now monitoring the market!
⏰ Started: {self.local_now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': message.strip(),
                'parse_mode': 'Markdown'
            }
            
            requests.post(url, data=data, timeout=10)
            
        except Exception as e:
            self.logger.error(f"Error sending startup message: {e}")
    
    def start(self):
        """Start the signal bot"""
        self.logger.info("="*70)
        self.logger.info("🚀 OPTIMIZED TRADING SIGNAL BOT")
        self.logger.info("="*70)
        self.logger.info(f"📈 Monitoring: {', '.join(self.assets)}")
        self.logger.info(f"⏱️ Timeframes: M1, M5, M15 (Next Candle Predictions)")
        self.logger.info(f"🎯 Min Confidence: {self.config.get('min_confidence', 0.7):.0%}")
        self.logger.info(f"🔔 Telegram: {'Enabled' if self.config.get('telegram', {}).get('enabled') else 'Disabled'}")
        self.logger.info("="*70)
        
        # Send startup notification
        self.send_startup_message()
        
        # Initial analysis
        self.run_analysis()
        
        # Schedule analyses
        # M1: Every 1 minute
        schedule.every(1).minutes.do(self.run_analysis)
        
        # M5: Every 5 minutes  
        # M15: Every 15 minutes
        # (All covered by M1 scan since we check all timeframes)
        
        # Hourly summary
        schedule.every().hour.do(self.send_hourly_summary)
        
        self.logger.info("✅ Bot is running. Press Ctrl+C to stop.")
        self.logger.info("")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Signal bot stopped by user")
            self.send_shutdown_message()
    
    def send_hourly_summary(self):
        """Send hourly summary to Telegram"""
        try:
            telegram_config = self.config.get('telegram', {})
            if not telegram_config.get('enabled', False):
                return
            
            token = telegram_config.get('token')
            chat_id = telegram_config.get('chat_id')
            
            if not token or not chat_id:
                return
            
            uptime = self.local_now() - self.session_start
            
            # Get signal statistics
            stats = self.db.get_signal_statistics()
            
            message = f"""
📊 *HOURLY SUMMARY*

🎯 Signals Sent: {self.signals_sent}
✅ Wins: {stats['wins']}
❌ Losses: {stats['losses']}
⏳ Pending: {stats['pending']}
📈 Win Rate: {stats['win_rate']:.1f}%

⏱️ Uptime: {str(uptime).split('.')[0]}
⏰ Time: {self.local_now().strftime('%H:%M:%S')}

✅ Bot is active and monitoring
"""
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': message.strip(),
                'parse_mode': 'Markdown'
            }
            
            requests.post(url, data=data, timeout=10)
            
        except Exception as e:
            self.logger.error(f"Error sending hourly summary: {e}")
    
    def send_shutdown_message(self):
        """Send shutdown notification"""
        try:
            telegram_config = self.config.get('telegram', {})
            if not telegram_config.get('enabled', False):
                return
            
            token = telegram_config.get('token')
            chat_id = telegram_config.get('chat_id')
            
            if not token or not chat_id:
                return
            
            uptime = self.local_now() - self.session_start
            
            # Get final signal statistics
            stats = self.db.get_signal_statistics()
            
            message = f"""
🛑 *SIGNAL BOT STOPPED*

📊 Total Signals: {self.signals_sent}
✅ Wins: {stats['wins']}
❌ Losses: {stats['losses']}
⏳ Pending: {stats['pending']}
📈 Win Rate: {stats['win_rate']:.1f}%

⏱️ Session Duration: {str(uptime).split('.')[0]}
⏰ Stopped: {self.local_now().strftime('%Y-%m-%d %H:%M:%S')}

👋 Bot has been shut down
"""
            
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': message.strip(),
                'parse_mode': 'Markdown'
            }
            
            requests.post(url, data=data, timeout=10)
            
        except Exception as e:
            self.logger.error(f"Error sending shutdown message: {e}")


def send_telegram_message(text):
    """Send a plain text Telegram message (for start/stop notifications)"""
    try:
        token = "8432186447:AAHStxiGWnqeLAk9XmeCS-ExwEuNSUsXWqg"
        chat_id = "1454104544"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        logging.error(f"Error sending Telegram message: {e}")


def main():
    """Main entry point"""
    bot_running = False  # Track whether the bot is active

    # TEST Telegram message at startup
    send_telegram_message("✅ Bot container started successfully.")
    logging.info("✅ Bot container started successfully.")

    try:
        while True:
            now = datetime.now() + timedelta(hours=1)
            current_time = now.time()

            # Define working hours
            start_time = datetime.strptime("07:00", "%H:%M").time()
            end_time = datetime.strptime("22:00", "%H:%M").time()

            if start_time <= current_time <= end_time:
                if not bot_running:
                    msg = f"🚀 *Trading Bot Started*\nTime: {now.strftime('%H:%M')} (local)"
                    send_telegram_message(msg)
                    logging.info(f"⏱ {now.strftime('%H:%M')} - Within working hours. Starting bot...")

                    bot = TradingSignalBot()
                    bot.start()
                    bot_running = True
                else:
                    logging.info(f"⏱ {now.strftime('%H:%M')} - Bot already running.")
            else:
                if bot_running:
                    msg = f"🌙 *Trading Bot Paused*\nTime: {now.strftime('%H:%M')} (outside working hours)"
                    send_telegram_message(msg)
                    logging.info(f"🌙 {now.strftime('%H:%M')} - Outside working hours. Bot paused.")
                    bot_running = False
                elif bot_running is False:  # only send once when paused
                    msg = "🌙 *Trading Bot is paused and will start at 07:00 local time.*"
                    send_telegram_message(msg)
                    logging.info(f"🌙 {now.strftime('%H:%M')} - Outside working hours. Waiting for 07:00.")
                    bot_running = None  # mark as message sent

            # Calculate time to next full hour
            minutes_until_next_hour = 60 - now.minute
            seconds_until_next_hour = minutes_until_next_hour * 60 - now.second
            next_check = now + timedelta(seconds=seconds_until_next_hour)
            logging.info(f"⏳ Next check scheduled at {next_check.strftime('%H:%M')}.")

            time.sleep(seconds_until_next_hour)

    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        send_telegram_message(f"❌ Fatal error: {e}")



if __name__ == "__main__":
    main()
