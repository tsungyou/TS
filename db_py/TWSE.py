import pandas as pd
import os
from tqdm import tqdm
import requests
import json
from datetime import datetime, timedelta
class TWSE(object):
    __slots__ = ("tw_symbol_4", "da_now", 'dirs')
    dirs = "../db"
    try:
        os.chdir(dirs)
    except FileNotFoundError:
        os.makedirs(dirs)
        os.chdir(dirs)
    
    def __init__(self):
        super().__init__()
        self.da_now = datetime.now()
        self.tw_symbol_4 = None
        with open("tw/symbol/symbol_4.json") as f:
            self.tw_symbol_4 = json.load(f)
        
        start, end = self.da_now.year, 2017
        for year in range(start, end, -1):
            for func in ['price', 'pb']:
                self.get_TWSE_yearly(year=year, func=func)
        self.convert_to_pdata_pe_ratio()
        self.convert_to_pdata_price()
        print("get TWSE index data...")
        self.get_TWSE_TWSE()

    def get_TWSE_TWSE(self):
        def convert_to_gregorian(date_str):
            year, month, day = date_str.split('/')
            year = int(year) + 1911
            return f'{year}-{month}-{day}'

        list_list = []
        for year in range(2024, 2010, -1):
            limit_month = self.da_now.month + 1 if year == 2024 else 13
            for i in range(1, limit_month):
                month = f"0{i}" if i < 10 else i
                da = f"{year}{month}01"
                url_twse = f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={da}&response=json"
                response = requests.get(url_twse)
                dicts = response.json()
                list_list.append(dicts['data'])
        list_sum = sum(list_list, [])
        df = pd.DataFrame(list_sum, columns=dicts['fields'])
        df['日期'] = df['日期'].apply(convert_to_gregorian)
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index("日期", inplace=True)
        df.sort_index(ascending=True, inplace=True)
        df.to_parquet("tw/ind/TWSE.parquet")
        return None
    
    def _get_price_TWSE(self, ticker = '2330', year=2024):
        '''
        example url: f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20240101&stockNo=2330"
        '''
        limit_month = self.da_now.month+1 if year == 2024 else 13
        list_concat = [None] * (limit_month-1)
        for index, month in enumerate(range(1, limit_month)):
            try:
                month = f"0{month}" if month < 10 else month
                da = f"{year}{month}01"
                url_json = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={da}&stockNo={ticker}"
                response = requests.get(url_json)
                list_concat[index] = response.json()['data']
            except:
                if month == 1:
                    return None
                else:
                    list_concat = [item for item in list_concat if item is not None]                                        
                    break
        df_final = pd.DataFrame(sum(list_concat, []))
        df_final['ticker'] = ticker
        return df_final
    
    def _get_pbratio_TWSE(self, ticker='2330', year=2024):
        '''
        example url: f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date=20240101&stockNo=2330&response=json"
        '''
        limit_month = self.da_now.month+1 if year == 2024 else 13
        list_concat = [None] * (limit_month-1)
        for index, month in enumerate(range(1, limit_month)):
            try:
                month = f"0{month}" if month < 10 else month
                da = f"{year}{month}01"
                url_json = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date={da}&stockNo={ticker}&response=json"
                response = requests.get(url_json)
                list_concat[index] = response.json()['data']
            except:
                if month == 1:
                    return None
                else:
                    list_concat = [item for item in list_concat if item is not None]                                        
                    break
        df_final = pd.DataFrame(sum(list_concat, []))
        df_final['ticker'] = ticker
        return df_final
    
    def get_blockTrading_TWSE(self, ticker='2330', da=2024):
        '''
        example url:
        https://www.twse.com.tw/rwd/zh/block/BFIAUU_sd?stockNo=2330&date=2024&response=json&_=1720554887120
        '''
        pass

    def get_TWSE_yearly(self, year, func=None):
        def convert_to_datetime(date_str):
            date_str = date_str.strip()
            year = int(date_str[:3]) + 1911
            month = int(date_str[4:6])
            day = int(date_str[7:9])
            return datetime(year, month, day)
        def check_year_prefix(date_str):
            if isinstance(date_str, str) and len(date_str) >= 3:
                return date_str[:3] == "113"
            return False
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
        for i in range(0, len(list_tw_stock), per_loop):
            df_concat_by_ticker = []
            for ticker in tqdm(list(list_tw_stock[i:i+per_loop]), desc=f"{i}~{i+per_loop}"):
                list_ = func(ticker=ticker, year=year)
                df_concat_by_ticker.append(list_)
            df = pd.concat(df_concat_by_ticker, ignore_index=True)
            df.columns = col
            df_concat_by_year.append(df)
        df_final = pd.concat(df_concat_by_year, ignore_index=True)

        if db == "pb_ratio":
            df_final['index'] = df_final['da'].apply(check_year_prefix)
            df_final = df_final[df_final['index'] == True]
            df_final.set_index('da', inplace=True)
            df_final.index = df_final.index.to_series().apply(convert_to_datetime)
            df_final.sort_index(ascending=True, inplace=True)
        df_final.to_parquet(f"tw/{db}/{year}.parquet")
        return None

    def get_TWSE_price(self):
        '''
        example url: f"https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date=20240101&response=json"
        '''
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

        return True
    
    def convert_to_pdata_pe_ratio(self):
        print("start converting pe, pb from pb_ratio directory to pdata...")
        directory = 'tw/pb_ratio'
        list_ = os.listdir(directory)
        df_list = [pd.read_parquet(os.path.join(directory, i)) for i in list_]
        df_all = pd.concat(df_list)
        df_all['da'] = df_all.index
        df = df_all.drop_duplicates(subset=['ticker', 'da'])
        for values in ['pe', 'pb']:
            df_pivot = df.pivot(values=values, columns='ticker', index='da')

            df_pivot.replace("--", None, inplace=True)
            df_pivot.replace("-", None, inplace=True)
            df_pivot.ffill(inplace=True)
            df_pivot = df_pivot.loc['2018-01-01':(datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")]
            df_pivot.sort_index(ascending=True, inplace=True)
            df_pivot = df_pivot.apply(lambda x: x.str.replace(',', '').astype(float))
            df_pivot.dropna(how='all', axis=1, inplace=True)
            df_pivot.to_parquet(f"tw/pdata/{values}.parquet")
            print(f"store pdata of values {values} to tw/pdata/{values}.parquet successful")
        return None
    
    def convert_to_pdata_price(self):
        print("start converting tw/price to pdata...")
        directory = 'tw/price'
        list_ = os.listdir(directory)
        df_list = [pd.read_parquet(os.path.join(directory, i)) for i in list_]
        df_all = pd.concat(df_list)
        df_all['da1'] = df_all['da'].apply(lambda x: f"{int(x[:3])+1911}-{x[4:6]}-{x[7:9]}")
        df = df_all.drop_duplicates(subset=['ticker', 'da1'])
        df['da_check'] = df['da1'].astype(str).apply(lambda x: int(x[:4]))
        df_final = df[df['da_check'].isin([i for i in range(2018, self.da_now.year+2)])]

        for values in ['cl', 'op', 'vol(volume)', 'vol(turnover)']:
            df_pivot = df_final.pivot(values=values, columns='ticker', index='da1')
            df_pivot.replace("--", None, inplace=True)
            df_pivot.replace("-", None, inplace=True)
            df_pivot.ffill(inplace=True)
            df_pivot = df_pivot.loc['2018-01-01':(datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")]
            df_pivot.sort_index(ascending=True, inplace=True)
            df_pivot = df_pivot.apply(lambda x: x.str.replace(',', '').astype(float))
            df_pivot.dropna(how='all', axis=1, inplace=True)
            df_pivot.to_parquet(f"tw/pdata/{values}.parquet")
            print(f"store pdata of values {values} to tw/pdata/{values}.parquet successful")
            df_pivot_pct = df_pivot.pct_change().dropna()
            df_pivot.to_parquet(f"tw/pdata/{values}_pct.parquet")
            print(f"store pdata of values {values} to tw/pdata/{values}_pct.parquet successful")
        return None

if __name__ == "__main__":
    obj = TWSE()
    start, end, func = 2018, 2017, "price"
    for year in range(start, end, -1):
        df = obj.get_TWSE_yearly(year=year, func=func)
    obj.convert_to_pdata_pe_ratio()
    obj.convert_to_pdata_price()