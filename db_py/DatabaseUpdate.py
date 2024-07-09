import pandas as pd
import os
from tqdm import tqdm
from TWSE import ScrapeTWSE
from DatabaseFunctions import DatabaseFunctions
import json
class DbUpdateSafeVersion(DatabaseFunctions, ScrapeTWSE):
    __slots__ = ("tw_symbol_4")
    
    
    def __init__(self):
        super().__init__()
        self.tw_symbol_4 = None
        with open("tw/symbol/symbol_4.json") as f:
            self.tw_symbol_4 = json.load(f)
        for i in range(2021, 2018, -1):
            self.seperate_price(year=i)

    def seperate_price(self, year):
        print("tw stock price for", year, "started")
        col = ['da', "vol(volume)", "vol(turnover)", "op", "cl", "lo", "hi", "cl-op", "vol(amount)", "ticker"]
        df_concat = []
        list_tw_stock = [key for key, value in self.tw_symbol_4.items() if value=="TW"]
        for i in range(0, len(list(list_tw_stock)), 100):
            for ticker in tqdm(list(list_tw_stock[i:i+100]), desc=f"{i}~{i+100}"):
                list_ = self._get_price_TWSE(stock_symbol=ticker, year=year)
                df_concat.append(list_)
            df = pd.concat(df_concat)
            df.columns = col
            df.to_parquet(f"db/tw/price/{i+100}_{year}.parquet")
        return None    

    def get_pdata_from_safev(self):
        pass
if __name__ == "__main__":
    a = DbUpdateSafeVersion()