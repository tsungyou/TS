import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import numpy as np


class DbInitGoodInfo(object):
    __slots__ = ("ticker_list", "chromedriver_path")

    def __init__(self):
        pass


    def _get_balance_sheet(self, ticker=None):
        if ticker is None:
            print("ticker happens to be None, recheck that...")
            return None
        else:
            return True
        
    def get_ticker_list(self):
        url_stock_list_goodinfo = "https://goodinfo.tw/tw/Lib.js/StockTW_ID_NM_List.js?45482.4170601852"
        res = requests.get(url_stock_list_goodinfo)
        soup = BeautifulSoup(res.text, 'lxml')
        return soup
    

if __name__ == "__main__":
    obj = DbInitGoodInfo()
    print(obj.__slots__)