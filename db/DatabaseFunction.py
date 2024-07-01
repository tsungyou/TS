import os
import json
from datetime import datetime, timedelta
import yfinance as yf
class DatabaseFunctions(object):
    def __init__(self):
        
        if not os.path.exists("tw"):
            os.makedirs("tw")
            print("tw created...")
        if not os.path.exists("tw/symbol"):
            os.makedirs("tw/symbol")
            print("tw/symbol created...")
        if not os.path.exists("tw/price"):
            os.makedirs("tw/price")
            print("tw/price created...")
        if not os.path.exists("tw/pb_ratio"):
            os.makedirs("tw/pb_ratio")
            print("tw/pb_ratio created...")
        if not os.path.exists("tw/ind"):
            os.makedirs("tw/ind")
            print("tw/ind created...")
    def _save_json(self, dict_symbol, loc):
        with open(loc, "w") as f:
            json.dump(dict_symbol, f, indent=4)
        return True
    
    def _data_cleaning_get_tw_symbol_update(self, tr, col=False):
        if not col:
            return tr.text.replace("\n", "").replace("▲", "").replace("▼", "-").replace("+", "")
        if col:
            return tr.text.replace("▼", "")
        
    def _data_cleaning_price(self, data):
        if "/" in data:
            year, month, day = map(int, data.split('/'))
            return datetime.strptime(f"{year+1911}-{month}-{day}", "%Y-%m-%d")
        else:
            return data.replace(",", "")
        
    def _data_cleaning_pbratio(self, data):
        if isinstance(data, str) and "年" in data:
            year_tw, other = data.split("年")
            year = int(year_tw) + 1911
            month, o = other.split("月")
            day = o[:2]
            return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
        else:
            return data

        
    def test_tw_two(self, key):
        days = datetime.now() - timedelta(5)
        a = yf.download(f"{key}.TW", start=days)
        if len(a) == 0:
            return "TWO"
        else:
            return "TW"