"""
Optimized Signal Generator for Next Candle Predictions
Generates accurate trading signals for M1, M5, M15 timeframes
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional
import logging

@dataclass
class TradingSignal:
    asset: str
    timeframe: int
    signal_type: str
    direction: str  # BUY (CALL), SELL (PUT)
    strength: float
    confidence: float
    current_price: float
    target_price: Optional[float]
    entry_time: str
    expiry_time: str
    indicators: Dict

class SignalGenerator:
    def __init__(self, database):
        self.db = database
        self.logger = logging.getLogger(__name__)
        
        # Minimum data requirements per timeframe
        self.min_data_requirements = {
            1: 60,   # 1 hour of M1 data
            5: 100,  # ~8 hours of M5 data
            15: 100  # ~24 hours of M15 data
        }
    
    def get_price_data(self, asset: str, timeframe: int, limit: int = 150) -> pd.DataFrame:
        """Get price data as DataFrame for analysis"""
        try:
            data = self.db.get_latest_prices(asset, timeframe, limit)
            if not data:
                self.logger.warning(f"No data available for {asset} M{timeframe}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting price data for {asset} M{timeframe}: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all technical indicators at once for efficiency"""
        try:
            # Moving Averages
            df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
            df['ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
            df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
            df['sma_20'] = df['close'].rolling(window=20).mean()
            df['sma_50'] = df['close'].rolling(window=50).mean()
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # MACD
            ema_12 = df['close'].ewm(span=12, adjust=False).mean()
            ema_26 = df['close'].ewm(span=26, adjust=False).mean()
            df['macd'] = ema_12 - ema_26
            df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
            df['macd_hist'] = df['macd'] - df['macd_signal']
            
            # Bollinger Bands
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            std = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['bb_middle'] + (std * 2)
            df['bb_lower'] = df['bb_middle'] - (std * 2)
            df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle']
            
            # ATR for volatility
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = ranges.max(axis=1)
            df['atr'] = true_range.rolling(14).mean()
            
            # Stochastic Oscillator
            low_14 = df['low'].rolling(window=14).min()
            high_14 = df['high'].rolling(window=14).max()
            df['stoch_k'] = 100 * ((df['close'] - low_14) / (high_14 - low_14))
            df['stoch_d'] = df['stoch_k'].rolling(window=3).mean()
            
            # Price momentum
            df['momentum'] = df['close'].pct_change(periods=10) * 100
            
            # Volume analysis (if available)
            if df['volume'].sum() > 0:
                df['volume_sma'] = df['volume'].rolling(window=20).mean()
                df['volume_ratio'] = df['volume'] / df['volume_sma']
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating indicators: {e}")
            return df
    
    def detect_trend(self, df: pd.DataFrame) -> Dict:
        """Detect current market trend"""
        if len(df) < 50:
            return {'trend': 'NEUTRAL', 'strength': 0}
        
        current = df.iloc[-1]
        
        # EMA alignment
        ema_bullish = (current['ema_9'] > current['ema_21'] > current['ema_50'])
        ema_bearish = (current['ema_9'] < current['ema_21'] < current['ema_50'])
        
        # Price position relative to EMAs
        above_emas = current['close'] > current['ema_9'] > current['ema_21']
        below_emas = current['close'] < current['ema_9'] < current['ema_21']
        
        # Calculate trend strength using slope
        ema21_slope = (df['ema_21'].iloc[-1] - df['ema_21'].iloc[-10]) / df['ema_21'].iloc[-10]
        
        if ema_bullish and above_emas:
            strength = min(abs(ema21_slope) * 1000, 1.0)
            return {'trend': 'BULLISH', 'strength': strength}
        elif ema_bearish and below_emas:
            strength = min(abs(ema21_slope) * 1000, 1.0)
            return {'trend': 'BEARISH', 'strength': strength}
        else:
            return {'trend': 'NEUTRAL', 'strength': 0}
    
    def generate_comprehensive_signal(self, df: pd.DataFrame, asset: str, timeframe: int) -> Optional[TradingSignal]:
        """Generate signal using multiple confirmations"""
        
        min_required = self.min_data_requirements.get(timeframe, 100)
        if len(df) < min_required:
            self.logger.warning(f"Insufficient data for {asset} M{timeframe}: {len(df)}/{min_required}")
            return None
        
        # Calculate indicators
        df = self.calculate_technical_indicators(df)
        
        # Remove NaN rows
        df = df.dropna()
        if len(df) < 20:
            return None
        
        current = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Detect trend
        trend_info = self.detect_trend(df)
        
        # Signal scoring system
        buy_score = 0
        sell_score = 0
        signal_reasons = []
        
        # 1. EMA Crossover Signals (Weight: 2 points)
        if current['ema_9'] > current['ema_21'] and prev['ema_9'] <= prev['ema_21']:
            buy_score += 2
            signal_reasons.append("EMA 9/21 Bullish Cross")
        elif current['ema_9'] < current['ema_21'] and prev['ema_9'] >= prev['ema_21']:
            sell_score += 2
            signal_reasons.append("EMA 9/21 Bearish Cross")
        
        # 2. RSI Signals (Weight: 1.5 points)
        if current['rsi'] < 30 and prev['rsi'] >= 30:
            buy_score += 1.5
            signal_reasons.append(f"RSI Oversold: {current['rsi']:.1f}")
        elif current['rsi'] > 70 and prev['rsi'] <= 70:
            sell_score += 1.5
            signal_reasons.append(f"RSI Overbought: {current['rsi']:.1f}")
        elif 30 <= current['rsi'] < 40 and prev['rsi'] < current['rsi']:
            buy_score += 0.5
            signal_reasons.append("RSI Recovery from Oversold")
        elif 60 < current['rsi'] <= 70 and prev['rsi'] > current['rsi']:
            sell_score += 0.5
            signal_reasons.append("RSI Declining from Overbought")
        
        # 3. MACD Signals (Weight: 2 points)
        if current['macd'] > current['macd_signal'] and prev['macd'] <= prev['macd_signal']:
            buy_score += 2
            signal_reasons.append("MACD Bullish Cross")
        elif current['macd'] < current['macd_signal'] and prev['macd'] >= prev['macd_signal']:
            sell_score += 2
            signal_reasons.append("MACD Bearish Cross")
        
        # MACD Histogram momentum
        if current['macd_hist'] > 0 and current['macd_hist'] > prev['macd_hist']:
            buy_score += 0.5
        elif current['macd_hist'] < 0 and current['macd_hist'] < prev['macd_hist']:
            sell_score += 0.5
        
        # 4. Bollinger Bands (Weight: 1.5 points)
        if current['close'] < current['bb_lower'] and prev['close'] >= prev['bb_lower']:
            buy_score += 1.5
            signal_reasons.append("Price Below BB Lower")
        elif current['close'] > current['bb_upper'] and prev['close'] <= prev['bb_upper']:
            sell_score += 1.5
            signal_reasons.append("Price Above BB Upper")
        
        # BB Squeeze breakout
        if current['bb_width'] < 0.02:
            if current['close'] > current['bb_middle'] and current['close'] > prev['close']:
                buy_score += 0.5
                signal_reasons.append("BB Squeeze Bullish Breakout")
            elif current['close'] < current['bb_middle'] and current['close'] < prev['close']:
                sell_score += 0.5
                signal_reasons.append("BB Squeeze Bearish Breakout")
        
        # 5. Stochastic (Weight: 1 point)
        if current['stoch_k'] < 20 and current['stoch_k'] > prev['stoch_k']:
            buy_score += 1
            signal_reasons.append("Stochastic Oversold Recovery")
        elif current['stoch_k'] > 80 and current['stoch_k'] < prev['stoch_k']:
            sell_score += 1
            signal_reasons.append("Stochastic Overbought Decline")
        
        # 6. Trend Confirmation (Weight: 1.5 points)
        if trend_info['trend'] == 'BULLISH':
            buy_score += 1.5 * trend_info['strength']
            signal_reasons.append(f"Bullish Trend (Strength: {trend_info['strength']:.2f})")
        elif trend_info['trend'] == 'BEARISH':
            sell_score += 1.5 * trend_info['strength']
            signal_reasons.append(f"Bearish Trend (Strength: {trend_info['strength']:.2f})")
        
        # 7. Price Action (Weight: 1 point)
        if current['close'] > prev['close'] and current['close'] > current['open']:
            buy_score += 0.5
        elif current['close'] < prev['close'] and current['close'] < current['open']:
            sell_score += 0.5
        
        # 8. Momentum (Weight: 1 point)
        if current['momentum'] > 0.5:
            buy_score += 1
            signal_reasons.append(f"Strong Bullish Momentum: {current['momentum']:.2f}%")
        elif current['momentum'] < -0.5:
            sell_score += 1
            signal_reasons.append(f"Strong Bearish Momentum: {current['momentum']:.2f}%")
        
        # Calculate final signal
        total_score = buy_score + sell_score
        if total_score == 0:
            return None
        
        # Minimum score threshold
        min_score = 3.5 if timeframe == 1 else 4.0
        
        if buy_score > sell_score and buy_score >= min_score:
            direction = 'BUY'
            strength = min(buy_score / 10.0, 1.0)
            confidence = buy_score / (buy_score + sell_score)
        elif sell_score > buy_score and sell_score >= min_score:
            direction = 'SELL'
            strength = min(sell_score / 10.0, 1.0)
            confidence = sell_score / (buy_score + sell_score)
        else:
            return None

        # ✅ Enforce minimum strength requirement
        if strength <= 0.5:
            self.logger.info(f"Skipping {asset} M{timeframe}: Strength {strength:.2f} below 0.5")
            return None

        # Additional confidence filter
        if confidence < 0.60:
            return None

        
        # Calculate target price based on ATR
        atr_value = current['atr']
        if direction == 'BUY':
            target_price = current['close'] + (atr_value * 0.5)
        else:
            target_price = current['close'] - (atr_value * 0.5)
        
        # Calculate expiry time
        entry_time = datetime.now() + timedelta(hours=1)
        expiry_minutes = timeframe + 1  # Next candle + 1 minute buffer
        expiry_time = entry_time.timestamp() + (expiry_minutes * 60)
        
        signal = TradingSignal(
            asset=asset,
            timeframe=timeframe,
            signal_type="multi_indicator",
            direction=direction,
            strength=strength,
            confidence=confidence,
            current_price=float(current['close']),
            target_price=float(target_price),
            entry_time=entry_time.isoformat(),
            expiry_time=datetime.fromtimestamp(expiry_time).isoformat(),
            indicators={
                'rsi': float(current['rsi']),
                'macd': float(current['macd']),
                'macd_signal': float(current['macd_signal']),
                'trend': trend_info['trend'],
                'trend_strength': trend_info['strength'],
                'atr': float(atr_value),
                'buy_score': buy_score,
                'sell_score': sell_score,
                'reasons': signal_reasons
            }
        )
        
        return signal
    
    def analyze_asset(self, asset: str, timeframe: int) -> Optional[TradingSignal]:
        """Analyze asset and generate signal for next candle"""
        try:
            # Get price data
            df = self.get_price_data(asset, timeframe, 150)
            if df.empty:
                return None
            
            # Generate signal
            signal = self.generate_comprehensive_signal(df, asset, timeframe)
            
            if signal and signal.confidence >= 0.65:
                self.logger.info(
                    f"Signal: {asset} M{timeframe} | {signal.direction} | "
                    f"Confidence: {signal.confidence:.1%} | "
                    f"Reasons: {', '.join(signal.indicators['reasons'][:3])}"
                )
                return signal
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing {asset} M{timeframe}: {e}")
            return None
