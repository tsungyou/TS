import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import numpy as np


class ScrapeGoodInfo(object):
    __slots__ = ()

    def __init__(self):
        pass


    def _get_balance_sheet(self, ticker=None):
        if ticker is None:
            print("ticker happens to be None, recheck that...")
            return None
        else:
            return True