import pandas as pd
import os
from tqdm import tqdm
import requests
import json
from datetime import datetime
from DatabaseFunctions import DatabaseFunctions
class DbInitTWSE(DatabaseFunctions):
    __slots__ = ("tw_symbol_4", "da_now")
    os.chdir("../db")
    
    def __init__(self):
        super().__init__()
        df = pd.read_parquet("tw/pdata/close_pct.parquet")
        self.tw_symbol_4 = None
        with open("tw/symbol/symbol_4.json") as f:
            self.tw_symbol_4 = json.load(f)
        self.da_now = datetime.now()
    
    def _get_price_TWSE(self, ticker = '2330', year=2024):
        limit_month = self.da_now+1 if year == 2024 else 13
        list_concat = [None] * (limit_month-1)
        for index, month in enumerate(range(1, limit_month)):
            month = f"0{month}" if month < 10 else month
            da = f"{year}{month}01"
            url_json = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={da}&stockNo={ticker}"
            response = requests.get(url_json)
            list_concat[index] = response.json()['data']
        df_final = pd.DataFrame(sum(list_concat, []))
        df_final['ticker'] = ticker
        return df_final
    
    def _get_pbratio_TWSE(self, ticker='2330', year=2024):
        limit_month = self.da_now+1 if year == 2024 else 13
        list_concat = [None] * (limit_month-1)
        for index, month in enumerate(range(1, limit_month)):
            month = f"0{month}" if month < 10 else month
            da = f"{year}{month}01"
            url_json = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date={da}&stockNo={ticker}&response=json"
            response = requests.get(url_json)
            list_concat[index] = response.json()['data']
        df_final = pd.DataFrame(sum(list_concat, []))
        df_final['ticker'] = ticker
        return df_final
    
    def get_blockTrading_TWSE(self, ticker='2330', da=2024):
        pass

    def get_TWSE_yearly(self, year, func=None):
        per_loop = 100
        if func == "pb":
            db = "pb_ratio"
            func = self._get_pbratio_TWSE
            col = ['da', 'yield', 'year', 'pe', 'pb', 'fs_q', 'ticker']
        elif func == 'price':
            db = "price"
            func = self._get_price_TWSE
            col = ['da', "vol(volume)", "vol(turnover)", "op", "cl", "lo", "hi", "cl-op", "vol(amount)", "ticker"]

        print(f"{func.__name__} for {year}")
        list_tw_stock = [key for key, value in self.tw_symbol_4.items() if value=="TW"]
        df_concat_by_year = []
        for i in range(0, len(list(list_tw_stock)), per_loop):
            df_concat_by_ticker = []
            for ticker in tqdm(list(list_tw_stock[i:i+per_loop]), desc=f"{i}~{i+per_loop}"):
                list_ = func(ticker=ticker, year=year)
                df_concat_by_ticker.append(list_)
            df = pd.concat(df_concat_by_ticker, ignore_index=True)
            df.columns = col
            df_concat_by_year.append(df)
        df_final = pd.concat(df_concat_by_year, ignore_index=True)
        df_final.to_parquet(f"tw/{db}/{year}.parquet")
        return df_final

    # close, open, pct and others
    def get_price_TWSE_to_pdata(self, df_concat: pd.DataFrame):
        pivoted = df_concat.pivot(index='da', values="cl", columns="ticker").loc['2021-01-01':'2022-01-01']
        pivoted = pivoted.ffill()
        pivoted = pivoted.astype(float)
        pivoted.astype(float).to_parquet(f"pdata/close.parquet")
        pivoted_pct = pivoted.pct_change().dropna(how="all")
        pivoted_pct.to_parquet(f'tw/pdata/close_pct.parquet')
        return None
    
if __name__ == "__main__":
    a = DbInitTWSE()
    start, end, func = 2023, 2018, "pb"
    for year in range(start, end, -1):
        df = a.get_TWSE_yearly(year=year, func=func)
