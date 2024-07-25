import requests
from datetime import datetime
import pandas as pd

def get_stock_price(symbol='AAPL', start='2023-01-09', end='2023-01-20'):
    '''
    symbol, start_da, end_da
    '''
    uri = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}?apiKey=C1ArqE5IR4xXyDhViri8XNXGOdVQr1f7"
    res = requests.get(uri)
    data = res.json()
    df = pd.DataFrame(data['results'])
    df['t'] = df['t'].apply(lambda x: datetime.fromtimestamp(x/1000))
    df.set_index('t', inplace=True)
    return df

def get_option_by_ticker(symbol='TSLA', expiration='230113', type_='c', strike='150', start='2023-01-01', end='2023-01-11'):
    ticker = f"{symbol}{expiration}C000{strike}00"
    uri = f"https://api.polygon.io/v2/aggs/ticker/O:{ticker}/range/1/day/{start}/{end}?apiKey=C1ArqE5IR4xXyDhViri8XNXGOdVQr1f7"
    try:
        res = requests.get(uri)
        df = pd.DataFrame(res.json()['results'])
        df['t'] = df['t'].apply(lambda x: datetime.fromtimestamp(x/1000))
        df['strike'] = strike
        df['T'] = (pd.to_datetime(f"20{expiration}") - df['t']).apply(lambda x: x.days) / 365
        df.set_index('t', inplace=True)
        df = df.apply(lambda x: x.astype(float), axis=1)
        df['ticker'] = ticker
        df['symbol'] = symbol
        df['type'] = type_
        return df
    except KeyError as e:
        return res.json()