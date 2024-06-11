import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as st
from mathematics import MethematicFunctions
from alphalens import AlphaLens
# import abc
class Database(MethematicFunctions, AlphaLens):

    __slots__ = ("fft", "arima", "icir", "_close")

    def __init__(self):
        super().__init__()
        self._close = pd.read_parquet("../db/tw/Adj_close.parquet")
    
    @property
    def close(self):
        return self._close
    
    @close.setter
    def close(self, adj_close):
        self._close = adj_close

    @close.deleter
    def close(self):
        del self._close
        print('del complete')

    def __encapsulate(self):
        print("call encapsulated function")

    @staticmethod
    def plot_long_short_return(close):
        plt.plot(close)
        plt.show()

    @classmethod
    def plot_existing_factor(cls):
        adj_close = cls().close
        plt.plot(adj_close.loc[pd.to_datetime('2020-01-01'):]['1234.TW'])
        plt.title("1234.TW, 2020")
        plt.show()
        return None
    
    # factor processing methods
    def median_average_deviation(self, factor):
        df = factor
        return df
    
    # from factor to all
    def get_corresponding_weighting(self, factor):
        if isinstance(factor.index[0], str):
            factor.index = pd.to_datetime(factor.index)
        pass
a = Database()