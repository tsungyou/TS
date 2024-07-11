import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import warnings
from db_py.DbFunctions import DatabaseFunctions
from TWSE import TWSE
import os
from tqdm import tqdm



warnings.filterwarnings("ignore")
class DatabaseCreation(DatabaseFunctions, TWSE):
    
    __slots__ = ("directories")
    os.chdir("../db")
    def __init__(self):
        super().__init__()
        
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

if __name__ == "__main__":
    a = DatabaseCreation()