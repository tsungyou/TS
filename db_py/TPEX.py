

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os

class TPEX(object):
    os.chdir('../db')
    __slots__ = ("year", "da_now")

    def __init__(self):
        self.da_now = datetime.now()
        self.year = self.da_now.year - 1911

    def get_two_stocks(self):
        '''
        example url:
        https://www.tpex.org.tw/web/stock/aftertrading/trading_volume/vol_rank.php?l=zh-tw
        '''
        pass

    def get_gretai50(self):
        '''
        https://www.tpex.org.tw/web/stock/iNdex_info/gretai50/inxhis/rihisqry_result.php?l=zh-tw&d=113/06/01&_=1720688879318
        '''
        ticker = "gretai50"
        list_list = []
        for year in range(self.year, 102, -1):
            limit_month = self.da_now.month+1 if year == self.year else 13
            for month in range(1, limit_month):
                month = f"0{month}" if month < 10 else month
                url_gretai50 = f"https://www.tpex.org.tw/web/stock/iNdex_info/{ticker}/inxhis/rihisqry_result.php?l=zh-tw&d={year}/{month}/01&_=1720688879318"
                response = requests.get(url_gretai50)
               
                data = response.json()['aaData']
                list_list.append(data)
        list_ = sum(list_list, [])
        df = pd.DataFrame(list_)
        df.to_parquet('tw/ind/gretai50.parquet')
        return None
    
    def get_two_index(self):
        '''
        https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41.php?l=zh-tw
        '''

        list_list = []
        print("start downloading TWOTCI index data from TPEX website...")
        for year in range(self.year, 102, -1):
            limit_month = self.da_now.month+1 if year == self.year else 13
            for month in range(1, limit_month):
                month = f"0{month}" if month < 10 else month
                url_twotci_index = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php?l=zh-tw&d={year}/{month}/01&_=1720721782315"
                response = requests.get(url_twotci_index)
               
                data = response.json()['aaData']
                list_list.append(data)
        list_ = sum(list_list, [])
        df = pd.DataFrame(list_, columns=['da', '成交股數(千股)', '金額(千元)', '筆數', "櫃買指數", "漲跌(點數)"])
        df.to_parquet('tw/ind/TWOTCI.parquet')
        print("finished, save file at db/tw/ind/TWOTCI.parquet")
        return None
    
if __name__ == "__main__":
    obj = TPEX()
    obj.get_two_index()