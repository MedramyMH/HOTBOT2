import logging
import os
from datetime import datetime, timedelta, time as dtime
from supabase import create_client, Client
import threading
import time as time_module

SUPABASE_URL = "https://oeinnehyvhyaxomjngwn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9laW5uZWh5dmh5YXhvbWpuZ3duIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI0MTg3NDQsImV4cCI6MjA3Nzk5NDc0NH0.4HM_qCaIPg_dkd7hli4D233OYa2YD3TR7C5Pa_YGJPo"

class PriceDatabase:
    def __init__(self):
        url = SUPABASE_URL
        key = SUPABASE_KEY

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

        self.supabase: Client = create_client(url, key)
        logging.info("✅ Connected to Supabase using Supabase Python client")

        # Start a background thread that resets data at midnight
        cleaner_thread = threading.Thread(target=self._schedule_midnight_cleanup, daemon=True)
        cleaner_thread.start()

    def _get_local_time(self):
        """Return current local time (UTC+1)"""
        return datetime.now() + timedelta(hours=1)

    def save_price_data(self, asset: str, price_data: dict, timeframe: int) -> bool:
        """Save price data to Supabase table (with local time)"""
        try:
            # Convert timestamp to Tunisia time (+1h)
            local_ts = price_data["time"]
        
            data = {
                "asset": asset,
                "timeframe": timeframe,
                "timestamp": local_ts,
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
                logging.error(f"Failed to insert: {response}")
                return False

        except Exception as e:
            logging.error(f"❌ Error saving {asset} data to Supabase: {e}")
            return False

    def get_database_stats(self):
        """Retrieve summary statistics"""
        try:
            response = self.supabase.table("price_data").select("*").execute()
            records = response.data or []
            total = len(records)
            unique_assets = len(set(r["asset"] for r in records))
            timestamps = [r["timestamp"] for r in records]
            return {
                "total_records": total,
                "unique_assets": unique_assets,
                "date_range": (min(timestamps) if timestamps else None,
                               max(timestamps) if timestamps else None)
            }
        except Exception as e:
            logging.error(f"Error getting stats from Supabase: {e}")
            return {"total_records": 0, "unique_assets": 0, "date_range": (None, None)}

    def _schedule_midnight_cleanup(self):
        """Run cleanup every midnight (local time)"""
        while True:
            now = self._get_local_time()
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            seconds_until_midnight = (tomorrow - now).total_seconds()

            logging.info(f"🕛 Next cleanup scheduled at: {tomorrow} (in {seconds_until_midnight:.0f}s)")
            time_module.sleep(seconds_until_midnight)

            try:
                self._clear_table()
            except Exception as e:
                logging.error(f"Error running midnight cleanup: {e}")

    def _clear_table(self):
        """Delete all rows from price_data"""
        try:
            logging.info("🧹 Deleting all rows from price_data (midnight reset)...")
            self.supabase.table("price_data").delete().neq("id", 0).execute()
            logging.info("✅ Table cleared successfully at midnight.")
        except Exception as e:
            logging.error(f"❌ Error clearing table: {e}")
