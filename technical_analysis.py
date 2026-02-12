import pandas as pd
import numpy as np
from ta import momentum, trend, volatility, volume
from sqlalchemy import text
from crypto import ensure_ohlcv_data, get_db_engine, fetch_mapping
from datetime import datetime, timedelta


def get_ohlcv_data(symbol, days=365):
    """Fetch OHLCV data from database for given symbol"""
    pair = symbol.upper() + 'USDT'
    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    query = """
        SELECT date, open, high, low, close, volume
        FROM ohlcv_data
        WHERE symbol = :symbol AND date >= :cutoff_date
        ORDER BY date ASC
    """

    engine = get_db_engine()
    df = pd.read_sql_query(text(query), engine, params={"symbol": pair, "cutoff_date": cutoff_date})

    if df.empty:
        ensure_ohlcv_data(symbol)
        df = pd.read_sql_query(text(query), engine, params={"symbol": pair, "cutoff_date": cutoff_date})
        if df.empty:
            return None

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')

    return df


def calculate_oscillators(df):
    """Calculate 5 oscillator indicators"""
    indicators = {}

    # 1. RSI (Relative Strength Index)
    indicators['rsi'] = momentum.RSIIndicator(close=df['close'], window=14).rsi()

    # 2. MACD (Moving Average Convergence Divergence)
    macd = trend.MACD(close=df['close'])
    indicators['macd'] = macd.macd()
    indicators['macd_signal'] = macd.macd_signal()
    indicators['macd_diff'] = macd.macd_diff()

    # 3. Stochastic Oscillator
    stoch = momentum.StochasticOscillator(
        high=df['high'],
        low=df['low'],
        close=df['close']
    )
    indicators['stoch_k'] = stoch.stoch()
    indicators['stoch_d'] = stoch.stoch_signal()

    # 4. ADX (Average Directional Index)
    adx = trend.ADXIndicator(high=df['high'], low=df['low'], close=df['close'])
    indicators['adx'] = adx.adx()

    # 5. CCI (Commodity Channel Index)
    indicators['cci'] = trend.CCIIndicator(
        high=df['high'],
        low=df['low'],
        close=df['close']
    ).cci()

    return indicators


def calculate_moving_averages(df):
    """Calculate 5 moving average indicators"""
    indicators = {}

    # 1. SMA (Simple Moving Average) - 20 and 50 period
    indicators['sma_20'] = trend.SMAIndicator(close=df['close'], window=20).sma_indicator()
    indicators['sma_50'] = trend.SMAIndicator(close=df['close'], window=50).sma_indicator()

    # 2. EMA (Exponential Moving Average) - 12 and 26 period
    indicators['ema_12'] = trend.EMAIndicator(close=df['close'], window=12).ema_indicator()
    indicators['ema_26'] = trend.EMAIndicator(close=df['close'], window=26).ema_indicator()

    # 3. WMA (Weighted Moving Average) - approximated with EMA
    indicators['wma_20'] = trend.WMAIndicator(close=df['close'], window=20).wma()

    # 4. Bollinger Bands
    bollinger = volatility.BollingerBands(close=df['close'])
    indicators['bb_high'] = bollinger.bollinger_hband()
    indicators['bb_mid'] = bollinger.bollinger_mavg()
    indicators['bb_low'] = bollinger.bollinger_lband()

    # 5. Volume Moving Average
    indicators['volume_ma'] = volume.VolumeWeightedAveragePrice(
        high=df['high'],
        low=df['low'],
        close=df['close'],
        volume=df['volume']
    ).volume_weighted_average_price()

    return indicators


def generate_oscillator_signals(indicators, current_price):
    """Generate buy/sell/hold signals from oscillators"""
    signals = []

    # RSI signals
    rsi = indicators['rsi'].iloc[-1]
    if pd.notna(rsi):
        if rsi < 30:
            signals.append({'indicator': 'RSI', 'signal': 'BUY', 'value': round(rsi, 2), 'reason': 'Oversold'})
        elif rsi > 70:
            signals.append({'indicator': 'RSI', 'signal': 'SELL', 'value': round(rsi, 2), 'reason': 'Overbought'})
        else:
            signals.append({'indicator': 'RSI', 'signal': 'HOLD', 'value': round(rsi, 2), 'reason': 'Neutral'})

    # MACD signals
    macd_diff = indicators['macd_diff'].iloc[-1]
    if pd.notna(macd_diff):
        if macd_diff > 0:
            signals.append({'indicator': 'MACD', 'signal': 'BUY', 'value': round(macd_diff, 6), 'reason': 'Bullish crossover'})
        else:
            signals.append({'indicator': 'MACD', 'signal': 'SELL', 'value': round(macd_diff, 6), 'reason': 'Bearish crossover'})

    # Stochastic signals
    stoch_k = indicators['stoch_k'].iloc[-1]
    if pd.notna(stoch_k):
        if stoch_k < 20:
            signals.append({'indicator': 'Stochastic', 'signal': 'BUY', 'value': round(stoch_k, 2), 'reason': 'Oversold'})
        elif stoch_k > 80:
            signals.append({'indicator': 'Stochastic', 'signal': 'SELL', 'value': round(stoch_k, 2), 'reason': 'Overbought'})
        else:
            signals.append({'indicator': 'Stochastic', 'signal': 'HOLD', 'value': round(stoch_k, 2), 'reason': 'Neutral'})

    # ADX signals
    adx = indicators['adx'].iloc[-1]
    if pd.notna(adx):
        if adx > 25:
            signals.append({'indicator': 'ADX', 'signal': 'HOLD', 'value': round(adx, 2), 'reason': 'Strong trend'})
        else:
            signals.append({'indicator': 'ADX', 'signal': 'HOLD', 'value': round(adx, 2), 'reason': 'Weak trend'})

    # CCI signals
    cci = indicators['cci'].iloc[-1]
    if pd.notna(cci):
        if cci < -100:
            signals.append({'indicator': 'CCI', 'signal': 'BUY', 'value': round(cci, 2), 'reason': 'Oversold'})
        elif cci > 100:
            signals.append({'indicator': 'CCI', 'signal': 'SELL', 'value': round(cci, 2), 'reason': 'Overbought'})
        else:
            signals.append({'indicator': 'CCI', 'signal': 'HOLD', 'value': round(cci, 2), 'reason': 'Neutral'})

    return signals


def generate_ma_signals(indicators, current_price):
    """Generate buy/sell/hold signals from moving averages"""
    signals = []

    # SMA signals
    sma_20 = indicators['sma_20'].iloc[-1]
    sma_50 = indicators['sma_50'].iloc[-1]
    if pd.notna(sma_20) and pd.notna(sma_50):
        if current_price > sma_20 > sma_50:
            signals.append({'indicator': 'SMA', 'signal': 'BUY', 'value': f'20: {round(sma_20, 2)}, 50: {round(sma_50, 2)}', 'reason': 'Price above SMAs'})
        elif current_price < sma_20 < sma_50:
            signals.append({'indicator': 'SMA', 'signal': 'SELL', 'value': f'20: {round(sma_20, 2)}, 50: {round(sma_50, 2)}', 'reason': 'Price below SMAs'})
        else:
            signals.append({'indicator': 'SMA', 'signal': 'HOLD', 'value': f'20: {round(sma_20, 2)}, 50: {round(sma_50, 2)}', 'reason': 'Mixed signals'})

    # EMA signals
    ema_12 = indicators['ema_12'].iloc[-1]
    ema_26 = indicators['ema_26'].iloc[-1]
    if pd.notna(ema_12) and pd.notna(ema_26):
        if ema_12 > ema_26:
            signals.append({'indicator': 'EMA', 'signal': 'BUY', 'value': f'12: {round(ema_12, 2)}, 26: {round(ema_26, 2)}', 'reason': 'Bullish crossover'})
        else:
            signals.append({'indicator': 'EMA', 'signal': 'SELL', 'value': f'12: {round(ema_12, 2)}, 26: {round(ema_26, 2)}', 'reason': 'Bearish crossover'})

    # WMA signals
    wma_20 = indicators['wma_20'].iloc[-1]
    if pd.notna(wma_20):
        if current_price > wma_20:
            signals.append({'indicator': 'WMA', 'signal': 'BUY', 'value': round(wma_20, 2), 'reason': 'Price above WMA'})
        else:
            signals.append({'indicator': 'WMA', 'signal': 'SELL', 'value': round(wma_20, 2), 'reason': 'Price below WMA'})

    # Bollinger Bands signals
    bb_high = indicators['bb_high'].iloc[-1]
    bb_low = indicators['bb_low'].iloc[-1]
    bb_mid = indicators['bb_mid'].iloc[-1]
    if pd.notna(bb_high) and pd.notna(bb_low) and pd.notna(bb_mid):
        if current_price < bb_low:
            signals.append({'indicator': 'Bollinger Bands', 'signal': 'BUY', 'value': f'L: {round(bb_low, 2)}, M: {round(bb_mid, 2)}, H: {round(bb_high, 2)}', 'reason': 'Price below lower band'})
        elif current_price > bb_high:
            signals.append({'indicator': 'Bollinger Bands', 'signal': 'SELL', 'value': f'L: {round(bb_low, 2)}, M: {round(bb_mid, 2)}, H: {round(bb_high, 2)}', 'reason': 'Price above upper band'})
        else:
            signals.append({'indicator': 'Bollinger Bands', 'signal': 'HOLD', 'value': f'L: {round(bb_low, 2)}, M: {round(bb_mid, 2)}, H: {round(bb_high, 2)}', 'reason': 'Within bands'})

    # Volume MA signal
    vwap = indicators['volume_ma'].iloc[-1]
    if pd.notna(vwap):
        if current_price > vwap:
            signals.append({'indicator': 'VWAP', 'signal': 'BUY', 'value': round(vwap, 2), 'reason': 'Price above VWAP'})
        else:
            signals.append({'indicator': 'VWAP', 'signal': 'SELL', 'value': round(vwap, 2), 'reason': 'Price below VWAP'})

    return signals


def calculate_overall_signal(all_signals):
    """Calculate overall buy/sell/hold recommendation"""
    buy_count = sum(1 for s in all_signals if s['signal'] == 'BUY')
    sell_count = sum(1 for s in all_signals if s['signal'] == 'SELL')
    hold_count = sum(1 for s in all_signals if s['signal'] == 'HOLD')

    total = buy_count + sell_count + hold_count

    if total == 0:
        return {'recommendation': 'HOLD', 'strength': 0, 'buy_count': 0, 'sell_count': 0, 'hold_count': 0}

    buy_percentage = (buy_count / total) * 100
    sell_percentage = (sell_count / total) * 100
    hold_percentage = (hold_count / total) * 100

    # Determine recommendation based on which has the most signals
    if buy_count > sell_count and buy_count > hold_count:
        # BUY wins
        recommendation = 'STRONG BUY' if buy_percentage > 70 else 'BUY'
        strength = buy_percentage
    elif sell_count > buy_count and sell_count > hold_count:
        # SELL wins
        recommendation = 'STRONG SELL' if sell_percentage > 70 else 'SELL'
        strength = sell_percentage
    elif buy_count > sell_count:
        # BUY > SELL but HOLD is highest
        recommendation = 'WEAK BUY'
        strength = buy_percentage
    elif sell_count > buy_count:
        # SELL > BUY but HOLD is highest
        recommendation = 'WEAK SELL'
        strength = sell_percentage
    else:
        # Equal BUY and SELL, or HOLD dominates with equal BUY/SELL
        recommendation = 'HOLD'
        # Use the highest percentage for strength
        strength = max(buy_percentage, sell_percentage, hold_percentage)

    return {
        'recommendation': recommendation,
        'strength': round(strength, 1),
        'buy_count': buy_count,
        'sell_count': sell_count,
        'hold_count': hold_count
    }


def calculate_support_resistance(df, current_price):
    """Calculate support and resistance levels"""
    # Get recent highs and lows
    recent_days = min(30, len(df))
    recent_df = df.tail(recent_days)

    # Find local maxima (resistance) and minima (support)
    highs = recent_df['high'].values
    lows = recent_df['low'].values

    # Simple support/resistance calculation
    resistance_levels = []
    support_levels = []

    # Get highest high as resistance
    r1 = np.max(highs)
    if r1 > current_price:
        resistance_levels.append(round(r1, 2))

    # Get lowest low as support
    s1 = np.min(lows)
    if s1 < current_price:
        support_levels.append(round(s1, 2))

    # Calculate pivot points
    high = df['high'].iloc[-1]
    low = df['low'].iloc[-1]
    close = df['close'].iloc[-1]

    pivot = (high + low + close) / 3
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)

    if r2 > current_price:
        resistance_levels.append(round(r2, 2))
    if s2 < current_price:
        support_levels.append(round(s2, 2))

    # Remove duplicates and sort
    resistance_levels = sorted(list(set(resistance_levels)))[:3]
    support_levels = sorted(list(set(support_levels)), reverse=True)[:3]

    return {
        'resistance': resistance_levels,
        'support': support_levels,
        'pivot': round(pivot, 2)
    }


def generate_trading_suggestion(overall, current_price, levels):
    """Generate trading suggestions based on analysis"""
    recommendation = overall['recommendation']

    suggestions = {
        'action': '',
        'entry_price': None,
        'stop_loss': None,
        'take_profit': None,
        'risk_reward': None
    }

    if 'BUY' in recommendation:
        suggestions['action'] = 'Consider buying'
        suggestions['entry_price'] = round(current_price, 2)

        # Stop loss below nearest support
        if levels['support']:
            suggestions['stop_loss'] = round(levels['support'][0] * 0.98, 2)
        else:
            suggestions['stop_loss'] = round(current_price * 0.95, 2)

        # Take profit at nearest resistance
        if levels['resistance']:
            suggestions['take_profit'] = round(levels['resistance'][0], 2)
        else:
            suggestions['take_profit'] = round(current_price * 1.10, 2)

    elif 'SELL' in recommendation:
        suggestions['action'] = 'Consider selling or shorting'
        suggestions['entry_price'] = round(current_price, 2)

        # Stop loss above nearest resistance
        if levels['resistance']:
            suggestions['stop_loss'] = round(levels['resistance'][0] * 1.02, 2)
        else:
            suggestions['stop_loss'] = round(current_price * 1.05, 2)

        # Take profit at nearest support
        if levels['support']:
            suggestions['take_profit'] = round(levels['support'][0], 2)
        else:
            suggestions['take_profit'] = round(current_price * 0.90, 2)

    else:
        suggestions['action'] = 'Hold or wait for clearer signals'

    # Calculate risk/reward ratio
    if suggestions['stop_loss'] and suggestions['take_profit']:
        risk = abs(suggestions['entry_price'] - suggestions['stop_loss'])
        reward = abs(suggestions['take_profit'] - suggestions['entry_price'])
        if risk > 0:
            suggestions['risk_reward'] = round(reward / risk, 2)

    return suggestions


def analyze_symbol(symbol, timeframe='1y'):
    """Main function to perform technical analysis"""
    # Use 1 year of data for comprehensive analysis
    days = 365

    # Get OHLCV data
    df = get_ohlcv_data(symbol, days=days)

    if df is None or len(df) < 50:
        return {'error': 'Insufficient data for analysis'}

    # Get current price
    coin = fetch_mapping(
        "SELECT price, market_cap, volume_24h FROM top_coins WHERE UPPER(symbol) = :symbol",
        {"symbol": symbol.upper()}
    )

    if not coin:
        return {'error': 'Coin not found'}

    current_price = coin['price']

    # Calculate indicators
    oscillators = calculate_oscillators(df)
    moving_averages = calculate_moving_averages(df)

    # Merge all indicators
    all_indicators = {**oscillators, **moving_averages}

    # Generate signals
    oscillator_signals = generate_oscillator_signals(oscillators, current_price)
    ma_signals = generate_ma_signals(moving_averages, current_price)

    all_signals = oscillator_signals + ma_signals

    # Calculate overall recommendation
    overall = calculate_overall_signal(all_signals)

    # Calculate support/resistance levels
    levels = calculate_support_resistance(df, current_price)

    # Generate trading suggestions
    trading = generate_trading_suggestion(overall, current_price, levels)

    # Get chart data (last 50 data points)
    chart_data = df.tail(50)[['date', 'close']].to_dict('records')
    chart_data = [{
        'date': d['date'].strftime('%Y-%m-%d'),
        'price': round(d['close'], 2)
    } for d in chart_data]

    return {
        'symbol': symbol.upper(),
        'current_price': current_price,
        'market_cap': coin['market_cap'],
        'volume_24h': coin['volume_24h'],
        'timeframe': timeframe,
        'oscillators': oscillator_signals,
        'moving_averages': ma_signals,
        'overall': overall,
        'levels': levels,
        'trading': trading,
        'chart_data': chart_data,
        'timestamp': datetime.now().isoformat()
    }
