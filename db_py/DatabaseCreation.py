import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import warnings
from DatabaseFunctions import DatabaseFunctions
from TWSE import ScrapeTWSE
import os
from tqdm import tqdm

warnings.filterwarnings("ignore")
class DatabaseCreation(DatabaseFunctions, ScrapeTWSE):
    
    __slots__ = ("tw_symbol_4", "tw_symbol_6", "directories")
    
    
    def __init__(self):
        super().__init__()
        self.directories = ['tw/pb_ratio', "tw/price"]
        self.tw_symbol_4 = None
        self.tw_symbol_6 = None
        if not os.path.exists("tw/symbol/symbol_4.json"):
            self.get_tw_symbol()
        with open("tw/symbol/symbol_4.json") as f:
            self.tw_symbol_4 = json.load(f)
        with open("tw/symbol/symbol_6.json") as f:
            self.tw_symbol_6 = json.load(f)

        print("==========start database init tw price...==========")
        for year in range(2021, 2019, -1):
            pass 
            self.loop_price_TWSE(year=year)
        print("==========start database init tw pbratio...==========")
        for year in range(2024, 2017, -1):
            self.loop_pbratio_TWSE(year=year)
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
    
    # convert all price, pbratio datra to float datatype
    def get_ind_pdata_parquet(self):
        parquet_files = [os.path.join(self.directories[0], f) for f in os.listdir(self.directories[0]) if f.endswith(".parquet")] 
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