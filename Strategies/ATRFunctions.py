import pandas as pd
import numpy as np


class ATRFunctions(object):
    def __init__(self):
        pass


    def strategy_open_1m_reversal(df, sep, us_open, daily_open, timeframe=5):
        '''
        Input:
        df: dataframe groupby da, intraday data for each day
        sep: seperator, time for us_open, specified by summertime
        us_open: us_open price
        daily_open: daily_open_price
        timeframe=5: price data freq, default by 5min data, which Metatrader5 provided with back to 2023-02

        By default there's no tp and sl for this strategy, so neither parameter for take profit nor stop loss points is specified.
        '''
        trade_type = "open_1m"
        one_tick_before_open = 15 if timeframe == 5 else 25
        before_us_df = us_open - daily_open > 0
        df.loc[df['time'] == sep, 'signal'] = 1 if before_us_df else -1
        df.loc[df['time'] == sep.replace("30", str(one_tick_before_open)), 'trade_type'] = trade_type
        return df

def strategy_open_30m_trend(df, sep, hour, us_open, timframe=5, tp=1000, sl=500, _digits=0.1):
    '''
    df: dataframe groupby da, intraday data for each day
    sep: seperator, time for us_open, specified by summertime
    us_open: us_open price
    hour: us_open hour in Taiwan, specified by summertime identifier
    timeframe=5: price data freq, default by 5min data, which Metatrader5 provided with back to 2023-02
    tp: take profit point
    sp: stop loss point
    _digits: smallest digits for the pairs/assets, S=10000.1 => _digits=0.1
    '''
    tp_price = tp*_digits
    sl_price = sl*_digits
    df_10 = df[df['time'] >= f"{hour+1}:00:00"]
    open_price_10 = df_10['op'].iloc[0]
    us_open_30 = (open_price_10 - us_open) > 0
    signal = 0
    open_position_price = 0
    close_position_price = 0
    # long
    if not us_open_30:
        upper_breakout = df[(df['time'] < f"{hour+1}:00:00") & (df['time'] >= sep)]['hi'].max()
        for _, row in df_10.iterrows():
            time = row['time']
            # breakout
            if row['cl'] > upper_breakout and signal == 0:
                open_position_price = row['cl']
                signal = 1
                df.loc[(df['time'] == time), "signal"] = signal
                sl = open_position_price - sl_price
                tp = open_position_price + tp_price
            # positionsTotal() == 1
            elif signal == 1:
                # tp
                if row['cl'] >= tp or row['cl'] <= sl:
                    df.loc[(df['time'] == time), "signal"] = 0
                    close_position_price = row['cl']
                    profit = close_position_price - open_position_price
                    signal = -1
                # nothing
                else:
                    df.loc[(df['time'] == time), "signal"] = 1
                # sl
                ##########
            # position closed
            elif signal == -1:
                return df
            # no position yet, no signal at the time
            else:
                continue
    # short case
    else:
        lower_breakout = df[(df['time'] < f"{hour+1}:00:00") & (df['time'] >= sep)]['cl'].min()
        for _, row in df_10.iterrows():
            time = row['time']
            # breakout
            if row['cl'] < lower_breakout and signal == 0:
                open_position_price = row['cl']
                signal = -1
                df.loc[(df['time'] == time), "signal"] = signal
                df.loc[(df['time'] == time), 'trade_type'] = "open_30m"
                tp = open_position_price - tp_price
                sl = open_position_price + sl_price
            # positionsTotal() == 1
            elif signal == -1:
                # tp/sl
                if row['cl'] <= tp or row['cl'] >= sl:
                    df.loc[(df['time'] == time), "signal"] = 0 # "close" for check purpose
                    
                    close_position_price = row['cl']
                    profit = -(close_position_price - open_position_price)
                    signal = 1
                # nothing
                else:
                    df.loc[(df['time'] == time), "signal"] = signal
                    df.loc[(df['time'] == time), 'trade_type'] = "open_30m"
                # sl
                ##########
            # position closed
            elif signal == 1:
                df.loc[(df['time'] == time), "signal"] = 0 # "close"for check purpose
                return df
            # no position yet, no signal at the time
            else:
                continue
    return df