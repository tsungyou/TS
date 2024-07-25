import requests
from datetime import datetime
import pandas as pd

def get_implied_volatility(symbol='TSLA', strike_price=150, limit=10):
    '''
    get future (non historical) option status search by strike_price
    '''
    limit = 1000 if limit == -1 else limit
    # i.keys() for i in res.json()['results']: [dict_keys(['day', 'details', 'greeks', 'implied_volatility', 'open_interest', 'underlying_asset'])
    request = f"https://api.polygon.io/v3/snapshot/options/{symbol}?strike_price={strike_price}&limit={limit}&apiKey=C1ArqE5IR4xXyDhViri8XNXGOdVQr1f7"
    res = requests.get(request).json()
    implied_vol = [i['implied_volatility'] for i in res['results']]
    contract_type = [i['details']['contract_type'] for i in res['results']]
    expiration_date = [i['details']['expiration_date'] for i in res['results']]
    strike_price = [i['details']['strike_price'] for i in res['results']]
    delta = [i['greeks']['delta'] for i in res['results']]
    gamma = [i['greeks']['gamma'] for i in res['results']]
    theta = [i['greeks']['theta'] for i in res['results']]
    vega = [i['greeks']['vega'] for i in res['results']]
    open_interest = [i['open_interest'] for i in res['results']]
    last_updated = [datetime.fromtimestamp(i['day']['last_updated']/1000000000) for i in res['results'][:-2]]
    close = [i['day']['close'] for i in res['results'][:-2]]
    open = [i['day']['open'] for i in res['results'][:-2]]
    high = [i['day']['high'] for i in res['results'][:-2]]
    low = [i['day']['low'] for i in res['results'][:-2]]
    prev_close = [i['day']['previous_close'] for i in res['results'][:-2]]

    df = pd.DataFrame()
    df['implied_volatility'] = implied_vol
    df['contract_type'] = contract_type
    df['expiration_date'] = expiration_date
    df['strike_price'] = strike_price
    df['delta'] = delta
    df['gamma'] = gamma
    df['theta'] = theta
    df['vega'] = vega
    df['open_interest'] = open_interest
    df['last_updated'] = last_updated + [0, 0]
    df['close'] = close + [0, 0]
    df['open'] = open + [0, 0]
    df['high'] = high + [0, 0]
    df['low'] = low + [0, 0]
    df['prev_close'] = prev_close + [0, 0]
    return df
