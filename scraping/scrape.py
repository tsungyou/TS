import pandas as pd
import yfinance as yf
import requests
import time
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
class Scraper:
    url_otc_index = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41.php?l=zh-tw"

    
    def __init__(self):
        pass
    def get_tpex_selenium(self, url=url_otc_index):
        # service = Service('')
        # driver = webdriver.Chrome(service=service)
        # driver.get(url)
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "lxml")
        input_box = soup.find("input", {"id":"input_date"})
start = time.time()
time.sleep(1)
scraper1 = Scraper()
scraper1.get_tpex_selenium()
end = time.time()
print(end - start, "seconds")