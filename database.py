import logging
import threading
import time as time_module
from datetime import datetime, timedelta
from supabase import create_client, Client
from typing import Dict, List, Optional

SUPABASE_URL = "https://oeinnehyvhyaxomjngwn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9laW5uZWh5dmh5YXhvbWpuZ3duIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI0MTg3NDQsImV4cCI6MjA3Nzk5NDc0NH0.4HM_qCaIPg_dkd7hli4D233OYa2YD3TR7C5Pa_YGJPo"

class PriceDatabase:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.logger.info("✅ Connected to Supabase database")


    def _get_local_time(self):
        return datetime.now() + timedelta(hours=1)

    def save_price_data(self, asset: str, price_data: dict, timeframe: int) -> bool:
        """Save or update candle data"""
        try:
            data = {
                "asset": asset,
                "timeframe": timeframe,
                "timestamp": price_data["time"],
                "open": price_data["open"],
                "high": price_data["max"],
                "low": price_data["min"],
                "close": price_data["close"],
                "volume": price_data.get("volume", 0),
                "created_at": self._get_local_time().isoformat()
            }

            response = (
                self.supabase.table("price_data")
                .upsert(data, on_conflict="asset,timeframe,timestamp")
                .execute()
            )

            if response.data:
                return True
            self.logger.error(f"Insert failed: {response}")
            return False

        except Exception as e:
            self.logger.error(f"❌ Error saving {asset} data: {e}")
            return False

    def save_candle(self, asset: str, timeframe: int, candle_data: Dict) -> bool:
        return self.save_price_data(asset, candle_data, timeframe)

    def get_latest_prices(self, asset: str, timeframe: int, limit: int = 150) -> List[Dict]:
        """Fetch latest candles from Supabase"""
        try:
            response = (
                self.supabase.table("price_data")
                .select("timestamp, open, high, low, close, volume")
                .eq("asset", asset)
                .eq("timeframe", timeframe)
                .order("timestamp", desc=True)
                .limit(limit)
                .execute()
            )
            records = response.data or []
            return list(reversed(records))
        except Exception as e:
            self.logger.error(f"Error fetching latest prices for {asset} M{timeframe}: {e}")
            return []

    def get_database_stats(self):
        """Return DB stats"""
        try:
            response = self.supabase.table("price_data").select("asset,timestamp").execute()
            records = response.data or []
            total = len(records)
            assets = {r["asset"] for r in records if "asset" in r}
            timestamps = [r["timestamp"] for r in records if "timestamp" in r]
            return {
                "total_records": total,
                "unique_assets": len(assets),
                "date_range": (
                    min(timestamps) if timestamps else None,
                    max(timestamps) if timestamps else None
                )
            }
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return {"total_records": 0, "unique_assets": 0, "date_range": (None, None)}

    # ========== SIGNAL TRACKING METHODS ==========
    
    def save_signal(self, signal):
        """
        Save a trading signal to Supabase signals table.
        Returns the signal ID if successful, None otherwise.
        """
        try:
            data = {
                "asset": signal.asset,
                "timeframe": signal.timeframe,
                "direction": signal.direction,
                "confidence": float(signal.confidence),
                "entry_price": float(signal.current_price),
                "target_price": float(signal.target_price),
                "expiry_time": signal.expiry_time,
                "result": "PENDING",
                "created_at": self._get_local_time().isoformat()
            }

            response = (
                self.supabase.table("signals")
                .insert(data)
                .execute()
            )

            if response.data and len(response.data) > 0:
                signal_id = response.data[0].get('id')
                self.logger.info(
                    f"💾 Signal saved: {signal.asset} M{signal.timeframe} "
                    f"{signal.direction} (ID: {signal_id})"
                )
                return signal_id
            else:
                self.logger.error(f"Failed to save signal: {response}")
                return None

        except Exception as e:
            self.logger.error(f"❌ Error saving signal: {e}")
            return None

    def verify_signal_result(
        self, 
        signal_id: str, 
        asset: str, 
        timeframe: int, 
        direction: str, 
        entry_price: float, 
        expiry_time: str
    ):
        """
        Verify the result of a signal after expiry.
        Waits until expiry_time + 1 minute, then checks the outcome.
        """
        try:
            # Parse expiry time
            expiry_dt = datetime.fromisoformat(expiry_time)
            verification_time = expiry_dt + timedelta(minutes=1)
            
            # Calculate wait time
            now = self._get_local_time()
            wait_seconds = (verification_time - now).total_seconds()
            
            if wait_seconds > 0:
                self.logger.info(
                    f"⏳ Waiting {wait_seconds:.0f}s to verify signal {signal_id} "
                    f"({asset} M{timeframe})"
                )
                time_module.sleep(wait_seconds)
            
            # Fetch the closing price at expiry
            self.logger.info(f"🔍 Verifying signal {signal_id}...")
            
            # Get prices around the expiry time
            prices = self.get_latest_prices(asset, timeframe, limit=10)
            
            if not prices:
                self.logger.error(
                    f"❌ No price data available for verification of signal {signal_id}"
                )
                self._update_signal_result(signal_id, "ERROR", None)
                return
            
            # Find the candle closest to expiry time
            closest_candle = None
            min_time_diff = float('inf')
            
            for candle in prices:
                candle_time = datetime.fromisoformat(candle['timestamp'])
                time_diff = abs((candle_time - expiry_dt).total_seconds())
                
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_candle = candle
            
            if not closest_candle:
                self.logger.error(f"❌ Could not find candle for signal {signal_id}")
                self._update_signal_result(signal_id, "ERROR", None)
                return
            
            close_price = float(closest_candle['close'])
            
            # Determine result
            if direction == "BUY":
                result = "WIN" if close_price > entry_price else "LOSS"
            else:  # SELL
                result = "WIN" if close_price < entry_price else "LOSS"
            
            # Calculate profit/loss
            if direction == "BUY":
                pnl = close_price - entry_price
            else:
                pnl = entry_price - close_price
            
            # Update signal in database
            self._update_signal_result(signal_id, result, close_price)
            
            # Log result
            result_emoji = "✅" if result == "WIN" else "❌"
            self.logger.info(
                f"{result_emoji} Signal {signal_id} verified: {result} | "
                f"{asset} M{timeframe} {direction} | "
                f"Entry: {entry_price:.5f} → Close: {close_price:.5f} | "
                f"P/L: {pnl:+.5f}"
            )
            
        except Exception as e:
            self.logger.error(f"❌ Error verifying signal {signal_id}: {e}", exc_info=True)
            try:
                self._update_signal_result(signal_id, "ERROR", None)
            except:
                pass

    def _update_signal_result(self, signal_id: str, result: str, close_price):
        """Update the result field of a signal in the database"""
        try:
            update_data = {
                "result": result,
                "verified_at": self._get_local_time().isoformat()
            }
            
            if close_price is not None:
                update_data["close_price"] = float(close_price)
            
            response = (
                self.supabase.table("signals")
                .update(update_data)
                .eq("id", signal_id)
                .execute()
            )
            
            if response.data:
                self.logger.debug(f"✅ Updated signal {signal_id} result: {result}")
            else:
                self.logger.error(f"Failed to update signal {signal_id}: {response}")
                
        except Exception as e:
            self.logger.error(f"❌ Error updating signal {signal_id}: {e}")

    def get_signal_statistics(self) -> Dict:
        """Get statistics about signal performance"""
        try:
            response = self.supabase.table("signals").select("*").execute()
            signals = response.data or []
            
            total = len(signals)
            wins = len([s for s in signals if s.get('result') == 'WIN'])
            losses = len([s for s in signals if s.get('result') == 'LOSS'])
            pending = len([s for s in signals if s.get('result') == 'PENDING'])
            errors = len([s for s in signals if s.get('result') == 'ERROR'])
            
            win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
            
            return {
                "total_signals": total,
                "wins": wins,
                "losses": losses,
                "pending": pending,
                "errors": errors,
                "win_rate": win_rate
            }
            
        except Exception as e:
            self.logger.error(f"Error getting signal statistics: {e}")
            return {
                "total_signals": 0,
                "wins": 0,
                "losses": 0,
                "pending": 0,
                "errors": 0,
                "win_rate": 0
            }
