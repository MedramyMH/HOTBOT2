import logging
import os
from datetime import datetime, timedelta, time as dtime
from supabase import create_client, Client
import threading
import time as time_module
from typing import Dict, List, Optional

SUPABASE_URL = "https://oeinnehyvhyaxomjngwn.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9laW5uZWh5dmh5YXhvbWpuZ3duIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI0MTg3NDQsImV4cCI6MjA3Nzk5NDc0NH0.4HM_qCaIPg_dkd7hli4D233OYa2YD3TR7C5Pa_YGJPo"

class PriceDatabase:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
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

    def save_candle(self, asset: str, timeframe: int, candle_data: Dict) -> bool:
        """
        Alternative method name for saving candle data
        This provides compatibility with different naming conventions
        """
        return self.save_price_data(asset, candle_data, timeframe)
    
    def get_latest_prices(self, asset: str, timeframe: int, limit: int = 100) -> List[Dict]:
        """
        Get latest price data for a specific asset and timeframe
        
        Args:
            asset: Asset symbol
            timeframe: Timeframe in minutes
            limit: Number of records to return
            
        Returns:
            List of price data dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT timestamp, open, high, low, close, volume
                    FROM price_data 
                    WHERE asset = ? AND timeframe = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ''', (asset, timeframe, limit))
                
                rows = cursor.fetchall()
                result = []
                
                for row in rows:
                    result.append({
                        'timestamp': row['timestamp'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
                
                return result
                
        except Exception as e:
            logging.error(f"Error getting latest prices for {asset}: {e}")
            return []
    
    def get_price_data(self, asset: str, timeframe: int, start_time: int = None, end_time: int = None) -> List[Dict]:
        """
        Get price data for a specific asset and timeframe within a time range
        
        Args:
            asset: Asset symbol
            timeframe: Timeframe in minutes
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            
        Returns:
            List of price data dictionaries
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = '''
                    SELECT timestamp, open, high, low, close, volume
                    FROM price_data 
                    WHERE asset = ? AND timeframe = ?
                '''
                params = [asset, timeframe]
                
                if start_time:
                    query += ' AND timestamp >= ?'
                    params.append(start_time)
                
                if end_time:
                    query += ' AND timestamp <= ?'
                    params.append(end_time)
                
                query += ' ORDER BY timestamp ASC'
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                result = []
                for row in rows:
                    result.append({
                        'timestamp': row['timestamp'],
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
                
                return result
                
        except Exception as e:
            logging.error(f"Error getting price data for {asset}: {e}")
            return []
    
    def get_database_stats(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with database statistics
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total records
                cursor.execute('SELECT COUNT(*) FROM price_data')
                total_records = cursor.fetchone()[0]
                
                # Unique assets
                cursor.execute('SELECT COUNT(DISTINCT asset) FROM price_data')
                unique_assets = cursor.fetchone()[0]
                
                # Date range
                cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM price_data')
                min_max = cursor.fetchone()
                date_range = (min_max[0], min_max[1])
                
                # Asset counts
                cursor.execute('''
                    SELECT asset, COUNT(*) as count 
                    FROM price_data 
                    GROUP BY asset 
                    ORDER BY count DESC
                ''')
                asset_counts = [{'asset': row[0], 'count': row[1]} for row in cursor.fetchall()]
                
                return {
                    'total_records': total_records,
                    'unique_assets': unique_assets,
                    'date_range': date_range,
                    'asset_counts': asset_counts
                }
                
        except Exception as e:
            logging.error(f"Error getting database stats: {e}")
            return {
                'total_records': 0,
                'unique_assets': 0,
                'date_range': (None, None),
                'asset_counts': []
            }
    

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
