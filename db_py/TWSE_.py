import pandas as pd
import os
from tqdm import tqdm
import requests
import json
from datetime import datetime
from DatabaseFunctions import DatabaseFunctions
class DbInitTWSE(DatabaseFunctions):
    __slots__ = ("tw_symbol_4", "da_now")
    
    
    def __init__(self):
        super().__init__()
        self.tw_symbol_4 = None
        with open("../db/tw/symbol/symbol_4.json") as f:
            self.tw_symbol_4 = json.load(f)
        self.da_now = datetime.now()
    
    def _get_price_TWSE(self, stock_symbol = '2330', year=2024):
        list_concat = []
        limit_month = self.da_now.month if year == 2024 else 13
        df_final = pd.DataFrame()
        for month in range(1, limit_month):
            month = f"0{month}" if month < 10 else month
            da = f"{year}{month}01"

            url_json = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={da}&stockNo={stock_symbol}"
            response = requests.get(url_json)
            dicts = response.json()
            try:
                data = dicts['data']

                data = [[self._data_cleaning_price(i[j]) for j in range(len(data[0]))] for i in data]
                for sublist in data:
                    sublist.append(stock_symbol)
                df = pd.DataFrame(data)
                list_concat.append(df)
                    
                df_final = pd.concat(list_concat)
            except:
                continue
        return df_final
    
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
                list_ = func(stock_symbol=ticker, year=year)
                df_concat_by_ticker.append(list_)
            df = pd.concat(df_concat_by_ticker)
            df.columns = col
            df_concat_by_year.append(df)
        df_final = pd.concat(df_concat_by_year)
        df_final.to_parquet(f"../db/tw/{db}/{year}.parquet")
        return df_final

    def get_TWSE_all(self, start=2024, end=2018, func=None):
        df_all = []
        for year in range(start, end, -1):
            df = self.get_TWSE_yearly(year=year, func=func)
            df.drop_duplicates(subset=['da', 'ticker'], inplace=True)
            df_all.append(df)
        df_concat = pd.concat(df_all)
        df_concat.drop_duplicates(subset=['da', 'ticker'], inplace=True)
        df_concat.replace("--", None, inplace=True)
        return df_concat
    
    # close, open, pct and others
    def get_price_TWSE_to_pdata(self, df_concat: pd.DataFrame):
        pivoted = df_concat.pivot(index='da', values="cl", columns="ticker").loc['2021-01-01':'2022-01-01']
        pivoted = pivoted.ffill()
        pivoted = pivoted.astype(float)
        pivoted.astype(float).to_parquet(f"../db/tw/pdata/close.parquet")
        pivoted_pct = pivoted.pct_change().dropna(how="all")
        pivoted_pct.to_parquet(f'../db/tw/pdata/close_pct.parquet')
        return None
    
    def _get_pbratio_TWSE(self, stock_symbol='2330', year=2024):
        list_concat = []
        limit_month = self.da_now.month if year == 2024 else 13
        for month in range(1, limit_month):
            month = f"0{month}" if month < 10 else month
            da = f"{year}{month}01"
            url_json = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date={da}&stockNo={stock_symbol}&response=json"
            response = requests.get(url_json)
            dicts = response.json()
            try:
                data = dicts['data']
                data = [[self._data_cleaning_pbratio(j[i]) for i in range(len(j))] for j in data]
                for sublist in data:
                    sublist.append(stock_symbol) 
                df = pd.DataFrame(data)
                list_concat.append(df)
            except KeyError:
                if month == 1:
                    return None
                continue
        df_final = pd.concat(list_concat)
        return df_final
if __name__ == "__main__":
    a = DbInitTWSE()
    start, end, func = 2024, 2018, "pb"
    for year in range(start, end, -1):
        df = a.get_TWSE_yearly(year=year, func=func)
