import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import os
from tqdm import tqdm

class ScrapeTWSE(object):
    __slots__ = ()

    def __init__(self):
        pass

    def loop_price_TWSE(self, year=2024):
        print("tw stock price for", year, "started")
        col = ['da', "vol(volume)", "vol(turnover)", "op", "cl", "lo", "hi", "cl-op", "vol(amount)", "ticker"]
        df_concat = []
        list_tw_stock = [key for key, value in self.tw_symbol_4.items() if value=="TW"]
        for ticker in tqdm(list(list_tw_stock[:]), desc=f"Updating tw stock for {year}"):
            list_ = self._get_price_TWSE(stock_symbol=ticker, year=year)
            df_concat.append(list_)
        df = pd.concat(df_concat)
        df.columns = col
        df.to_parquet(f"tw/price/{year}.parquet")
        return None
    
    def _get_price_TWSE(self, stock_symbol = '2330', year=2024):
        list_concat = []
        limit_month = 7 if year == 2024 else 13
        df_final = pd.DataFrame()
        for i in range(1, limit_month):
            month = f"0{i}" if i < 10 else i
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
                    
    def _get_pbratio_TWSE(self, stock_symbol='2330', year=2024):
        list_concat = []
        limit_month = 7 if year == 2024 else 13
        for i in range(1, limit_month):
            month = f"0{i}" if i < 10 else i
            da = f"{year}{month}01"
            url_json = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date={da}&stockNo={stock_symbol}&response=json"
            response = requests.get(url_json)
            dicts = response.json()
            try:
                data = dicts['data']
                # fields = dicts['fields'] + ['股票代號']
                data = [[self._data_cleaning_pbratio(j[i]) for i in range(len(j))] for j in data]
                for sublist in data:
                    sublist.append(stock_symbol) 
                df = pd.DataFrame(data)
                list_concat.append(df)
            except KeyError:
                if i == 1:
                    return None
                continue
        df_final = pd.concat(list_concat)
        return df_final
    
    def loop_pbratio_TWSE(self, year=2024):
        print("tw stock pb ratio for", year, "started")
        col = ['da', "yield", "interest_year", "pe_ratio", "pb_ratio", "year/season", "ticker"]
        df_concat = []
        list_tw_stock = [key for key, value in self.tw_symbol_4.items() if value=="TW"]
        for ticker in tqdm(list(list_tw_stock), desc=f"Updating tw stock for {year}"):
            try:
                list_ = self._get_pbratio_TWSE(stock_symbol=ticker, year=year)
                print(list_)
                df_concat.append(list_)
            except ValueError:
                print(ticker, year)
        df = pd.concat(df_concat)
        df.columns = col
        df.to_parquet(f"tw/pb_ratio/{year}.parquet")
        return None 