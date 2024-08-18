import datetime
import time
from contextlib import closing

import numpy as np
import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config
import pandas_ta as ta


def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr


def calculate_signals(df, a=2, c=1):
    df.ta.ema(close=df['close'], length=20, append=True)
    df['atr'] = calculate_atr(df, period=c)
    df['nLoss'] = a * df['atr']
    df['src'] = df['close']
    df['xATRTrailingStop'] = np.nan

    # Initialize xATRTrailingStop
    if len(df) > 0:
        df.loc[df.index[0], 'xATRTrailingStop'] = df.loc[df.index[0], 'src']
    for i in range(1, len(df)):
        if df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] > \
                df.iloc[i - 1]['xATRTrailingStop']:
            df.loc[df.index[i], 'xATRTrailingStop'] = max(df.iloc[i - 1]['xATRTrailingStop'],
                                                          df.iloc[i]['src'] - df.iloc[i]['nLoss'])
        elif df.iloc[i]['src'] < df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] < \
                df.iloc[i - 1]['xATRTrailingStop']:
            df.loc[df.index[i], 'xATRTrailingStop'] = min(df.iloc[i - 1]['xATRTrailingStop'],
                                                          df.iloc[i]['src'] + df.iloc[i]['nLoss'])
        elif df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop']:
            df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] - df.iloc[i]['nLoss']
        else:
            df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] + df.iloc[i]['nLoss']

    df['pos'] = np.where((df['src'].shift(1) < df['xATRTrailingStop'].shift(1)) & (df['src'] > df['xATRTrailingStop']),
                         1, np.where(
            (df['src'].shift(1) > df['xATRTrailingStop'].shift(1)) & (df['src'] < df['xATRTrailingStop']), -1, np.nan))
    df['pos'] = df['pos'].ffill().fillna(0)
    return df


def get_applied_df_zerodha(instrument, broker, interval):
    from_datetime = datetime.datetime.now() - datetime.timedelta(days=15)
    to_datetime = datetime.datetime.now()
    candle_data = broker.historical_data(instrument['zerodha_instrument_token'], from_datetime, to_datetime,
                                         interval, continuous=False, oi=False)
    time.sleep(0.1)
    candle_data = pd.DataFrame(candle_data)
    if candle_data.empty:
        return pd.DataFrame()
    return calculate_signals(candle_data)


def get_applied_df_angel(instrument, broker, interval):
    from_datetime = datetime.datetime.now() - datetime.timedelta(days=15)
    to_datetime = datetime.datetime.now()
    from_datetime_formatted = from_datetime.strftime('%Y-%m-%d %H:%M')
    to_datetime_formatted = to_datetime.strftime('%Y-%m-%d %H:%M')
    candle_data = broker.getCandleData(historicDataParams={
        "exchange": instrument['angel_exchange_segment'],
        "symboltoken": instrument['angel_token'],
        "interval": interval,
        "fromdate": from_datetime_formatted,
        "todate": to_datetime_formatted
    })
    time.sleep(0.1)
    candle_data = pd.DataFrame(candle_data['data'])
    candle_data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    if candle_data.empty:
        return pd.DataFrame()
    return calculate_signals(candle_data)


def get_applied_df_shoonya(instrument, broker, interval):
    from_datetime = (datetime.datetime.today() - datetime.timedelta(days=15)).replace(second=0, microsecond=0)
    to_datetime = datetime.datetime.today().replace(second=0, microsecond=0)
    candle_data = broker.get_time_price_series(
        exchange=instrument['shoonya_exchange'],
        token=str(instrument['shoonya_token']),
        starttime=from_datetime.timestamp(),
        endtime=to_datetime.timestamp(),
        interval=interval
    )
    time.sleep(0.1)
    candle_data = pd.DataFrame(candle_data)
    candle_data = candle_data.iloc[::-1].reset_index(drop=True)
    if candle_data.empty:
        return pd.DataFrame()
    candle_data.columns = ['status', 'date', 'ssboe', 'open', 'high', 'low', 'close', 'vwap', 'interval', 'int_oi',
                           'volume', 'oi']
    data_types = {
        'open': 'float64',
        'high': 'float64',
        'low': 'float64',
        'close': 'float64',
        'volume': 'int64'
    }
    candle_data = candle_data.astype(data_types)
    return calculate_signals(candle_data)


class DataSignalsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    def get_active_broker(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT * FROM brokers WHERE broker_system_use_status = 1')
            active_broker = cursor.fetchone()
        return active_broker
