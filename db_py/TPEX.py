

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

class TPEX(object):
    __slots__ = ("year")
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
                url_gretai50 = f"https://www.tpex.org.tw/web/stock/iNdex_info/gretai50/inxhis/rihisqry_result.php?l=zh-tw&d={year}/{month}/01&_=1720688879318"
                response = requests.get(url_gretai50)
               
                data = response.json()['aaData']
                list_list.append(data)
        list_ = sum(list_list, [])
        df = pd.DataFrame(list_)
        df.to_parquet('tw/ind/gretai50.parquet')
        return None
    
    def get_two_index(self):
        '''
        
        '''
        pass