
import pandas as pd
import numpy as np

from db_py.Selenium import Selenium



class MOPS(Selenium):
    __slots__ = ()

    def __init__(self):
        super().__init__()
        print("chrome driverpath: ", self.chromedriver_path)
    def _get_BS_company(self, ticker='2330', da=2024):
        pass

    def _get_CF_company(self, ticker='2330', da=2024):
        pass