import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import warnings
from DatabaseFunctions import DatabaseFunctions
import os
from tqdm import tqdm

warnings.filterwarnings("ignore")
class DatabaseCreation(DatabaseFunctions):
    
    __slots__ = ("tw_symbol_4", "tw_symbol_6")

    
    def __init__(self):
        super().__init__()
        self.tw_symbol_4 = None
        self.tw_symbol_6 = None
        if not os.path.exists("tw/symbol/symbol_4.json"):
            self.get_tw_symbol()
        with open("tw/symbol/symbol_4.json") as f:
            self.tw_symbol_4 = json.load(f)
        with open("tw/symbol/symbol_6.json") as f:
            self.tw_symbol_6 = json.load(f)

        print("==========start database init tw...==========")
        for year in range(2019, 2015, -1):
            pass 
            # self.database_init_tw(year=year)
            # print("start database pbratio init tw...")
            # self.database_init_tw_pbratio(year=year)
            # print("start TWSE price init...")
            # self.get_TWSE_price()
        print("==========finished==========")
        self.get_ind_pdata_parquet()
    def get_tw_symbol(self):
        url_histock_stock_list = "https://histock.tw/stock/rank.aspx?p=all"
        response = requests.get(url_histock_stock_list)
        soup = BeautifulSoup(response.text, "lxml")
        td = soup.find_all("td")
        td_sep = [[self._data_cleaning_get_tw_symbol_update(td[j]) for j in range(i-13, i)] for i in range(13, len(td), 13)]

        dict_symbol = {td_sep[i][0]:td_sep[i][1] for i in range(len(td_sep))}
        dict_symbol_4 = {key:value for key, value in dict_symbol.items() if len(key) == 4}
        dict_symbol_6 = {key:value for key, value in dict_symbol.items() if len(key) == 6}
        for key, _ in dict_symbol_4.items():
            dict_symbol_4[key] = self.test_tw_two(key)

        self._save_json(dict_symbol_4, "tw/symbol/symbol_4.json")
        self._save_json(dict_symbol_6, "tw/symbol/symbol_6.json")
        return True
    def database_init_tw(self, year=2024):
        print("tw stock price for", year, "started")
        col = ['da', "vol(volume)", "vol(turnover)", "op", "cl", "lo", "hi", "cl-op", "vol(amount)", "ticker"]
        df_concat = []
        list_tw_stock = [key for key, value in self.tw_symbol_4.items() if value=="TW"]
        for ticker in tqdm(list(list_tw_stock[:]), desc=f"Updating tw stock for {year}"):
            list_ = self.get_tw_price(stock_symbol=ticker, year=year)
            df_concat.append(list_)
        df = pd.concat(df_concat)
        df.columns = col
        df.to_parquet(f"tw/price/{year}.parquet")
        return None
    
    def database_init_tw_pbratio(self, year=2024):
        print("tw stock pb ratio for", year, "started")
        col = ['da', "yield", "interest_year", "pe_ratio", "pb_ratio", "year/season", "ticker"]
        df_concat = []
        list_tw_stock = [key for key, value in self.tw_symbol_4.items() if value=="TW"]
        for ticker in tqdm(list(list_tw_stock), desc=f"Updating tw stock for {year}"):
            try:
                list_ = self.get_tw_pbratio(stock_symbol=ticker, year=year)
                df_concat.append(list_)
            except ValueError:
                print(ticker, year)
        df = pd.concat(df_concat)
        df.columns = col
        df.to_parquet(f"tw/pb_ratio/{year}.parquet")
        return None 
    def get_TWSE_price(self):
        print("init TWSE Index price...")
        list_concat = []
        for year in range(2024, 2015, -1):
            limit_month = 7 if year == 2024 else 13
            for i in range(1, limit_month):
                month = f"0{i}" if i < 10 else i
                da = f"{year}{month}01"
                url_twse = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={da}&response=json"
                response = requests.get(url_twse)
                dicts = response.json()

                data = dicts['data']
                data = [[self._data_cleaning_price(i[j]) for j in range(len(data[0]))] for i in data]
                for sublist in data:
                    sublist.append("TWSE Index")
                df = pd.DataFrame(data)
                list_concat.append(df)
        df_final = pd.concat(list_concat)
        df_final.to_parquet("tw/ind/TWSE.parquet")
        return True
    
    def get_tw_price(self, stock_symbol = '2330', year=2024):
        list_concat = []
        limit_month = 7 if year == 2024 else 13
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
                
            except KeyError:
                if i == 1:
                    return None
                continue
        df_final = pd.concat(list_concat)
        return df_final
                
    def get_tw_pbratio(self, stock_symbol='2330', year=2024):
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
    
    # convert all price, pbratio datra to float datatype
    def get_ind_pdata_parquet(self):
        directories = ['tw/pb_ratio', "tw/price"]
        parquet_files = [os.path.join(directories[1], f) for f in os.listdir(directories[1]) if f.endswith(".parquet")] 
        dfs = []

        for file in parquet_files:
            df = pd.read_parquet(file)
            dfs.append(df)
        df_concat = pd.concat(dfs)
        _ = self._parquet_to_pdata(df_concat, "cl", save=True)
        _ = self._parquet_to_pdata(df_concat, "op", save=True)
        _ = self._parquet_to_pdata(df_concat, "vol(turnover)", save=True)
        # _ = self._parquet_to_pdata(df_concat, "pe_ratio", save=True)
        # _ = self._parquet_to_pdata(df_concat, "pb_ratio", save=True)
        print("convert price.parquet to individual close, open, volume parquet file success...")
        return None
    def _parquet_to_pdata(self, df, values='cl', save=False):
        pivoted = df.pivot(index='da', values=values, columns="ticker")
        pivoted.replace("--", None, inplace=True)
        pivoted = pivoted.ffill()
        pivoted = pivoted.astype(float)
        '''
        vol(turnover): 成交金額
        vol(volume): 成交股數
        vol(amount): 成交筆數
        '''
        values_dict = {
            "cl": "close",
            "op": "open",
            "vol(turnover)": "volume",
            'vol(volume)': "volume(share)",
            "yield": "yield",
            "pe_ratio": "pe_ratio",
            "pb_ratio": "pb_ratio",
        }
        if save:
            pivoted.to_parquet(f'tw/pdata/{values_dict[values]}.parquet')
        return pivoted
    
if __name__ == "__main__":
    a = DatabaseCreation()