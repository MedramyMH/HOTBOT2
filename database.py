import logging
import threading
import time as time_module
from datetime import datetime, timedelta
from supabase import create_client, Client
from typing import Dict, List, Optional

# 🔑 Supabase credentials
SUPABASE_URL = "https://oeinnehyvhyaxomjngwn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9laW5uZWh5dmh5YXhvbWpuZ3duIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI0MTg3NDQsImV4cCI6MjA3Nzk5NDc0NH0.4HM_qCaIPg_dkd7hli4D233OYa2YD3TR7C5Pa_YGJPo"

class PriceDatabase:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

        self.supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.logger.info("✅ Connected to Supabase via Python client")

        # Start background cleanup thread
        cleaner_thread = threading.Thread(target=self._schedule_midnight_cleanup, daemon=True)
        cleaner_thread.start()

    def _get_local_time(self):
        """Return current local time (UTC+1 for Tunisia)"""
        return datetime.now() + timedelta(hours=1)

    # ==============================================================
    # 🔹 Save candles / price data
    # ==============================================================

    def save_price_data(self, asset: str, price_data: dict, timeframe: int) -> bool:
        """Save a candle record to Supabase table"""
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
            else:
                self.logger.error(f"❌ Failed to insert: {response}")
                return False

        except Exception as e:
            self.logger.error(f"❌ Error saving {asset} data to Supabase: {e}")
            return False

    def save_candle(self, asset: str, timeframe: int, candle_data: Dict) -> bool:
        """Alias for compatibility"""
        return self.save_price_data(asset, candle_data, timeframe)

    # ==============================================================
    # 🔹 Fetch recent candles
    # ==============================================================

    def get_latest_prices(self, asset: str, timeframe: int, limit: int = 150) -> List[Dict]:
        """Fetch the latest candles for an asset/timeframe"""
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
            return list(reversed(records))  # oldest → newest

        except Exception as e:
            self.logger.error(f"Error fetching latest prices for {asset} M{timeframe}: {e}")
            return []

    # ==============================================================
    # 🔹 Stats and maintenance
    # ==============================================================

    def get_database_stats(self):
        """Get record counts and date range"""
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
            self.logger.error(f"Error getting stats from Supabase: {e}")
            return {"total_records": 0, "unique_assets": 0, "date_range": (None, None)}
