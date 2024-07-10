
import pandas as pd
import numpy as np

from Selenium import Selenium



class MOPS(Selenium):
    __slots__ = ("test1", "test2")

    def __init__(self):
        super().__init__()
        print("chrome driverpath: ", self.chromedriver_path)
    def _get_BS_company(self, ticker='2330', da=2024):
        pass

    def _get_CF_company(self, ticker='2330', da=2024):
        pass



if __name__ == "__main__":
    mops = MOPS()
    print(mops.__slots__)