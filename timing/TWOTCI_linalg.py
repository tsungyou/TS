import pandas as pd
import os

import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
class Timing(object):
    __slots__ = ("df", "target", "train", "target_code", "train_codes", "_cn_price_parquet")
    def __init__(self):

        
        self.df = None
        self.target = None
        self.train = None
        self.target_code = "TWSE Index"
        self.train_codes = {
            '600167 CG Equity': "1", 
            '300249 CS Equity': "2"
        }
        self._cn_price_parquet = "../db/cn/"
        if self.df is None:
            self.get_cn_price_parquet()
            self.target = self.df[self.df['code'] == self.target_code][['da', 'cl', 'op', 'vol']].set_index("da").sort_index(ascending=True)
        self.train = self.df[self.df['code'].isin(list(self.train_codes.keys()))][['cl', 'code', 'da']]
        self.train = self.train.pivot(columns='code', index='da', values='cl')
        self.train['TWSE'] = self.target['cl']
        # auto action
        # self.plot_seperate_correlation()
        self.calc_SSE()
    def get_cn_price_parquet(self):
        list_ = os.listdir(self._cn_price_parquet)
        list_ = [os.path.join(self._cn_price_parquet, file) for file in list_]
        list_parquet = [pd.read_parquet(file) for file in list_]
        self.df = pd.concat(list_parquet)
        return None
    
    def plot_seperate_correlation(self, pct_change=True):
        train = self.train
        total = len(train.columns) - 1
        x = int(np.sqrt(total))
        y = int(total // x) if int(total % x) == 0 else int(total // x) + 1
        title_iter = list(train.columns)
        if pct_change:
            plot_df = train.pct_change(fill_method=None)
            plot_df['TWSE'] = plot_df['TWSE'].shift(1)
            plot_df = plot_df.iloc[2:, :]
        else:
            plot_df = train
        for i in range(1, total+1):
            plt.subplot(int(f"{x}{y}{i}"))
            plt.scatter(plot_df.iloc[:, i-1], plot_df.iloc[:, -1])
            plt.title(f"{title_iter[i-1]}, X")
            plt.grid()
        plt.tight_layout()
        plt.show()
        return None

    def calc_SSE(self):
        train = self.train.dropna()
        x = train.iloc[:, :-1]
        y = train.iloc[:, -1]
        X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=100)

        print("Start predicting...")
        lr_model = LinearRegression()
        lr_model.fit(X_train, y_train)
        print("End of fitting...")
        y_lr_train_pred = lr_model.predict(X_train)
        y_lr_test_pred = lr_model.predict(X_test)
        lr_train_mse = mean_squared_error(y_train, y_lr_train_pred)
        lr_test_mse = mean_squared_error(y_test, y_lr_test_pred)
        lr_train_r2 = r2_score(y_train, y_lr_train_pred)
        lr_test_r2 = r2_score(y_test, y_lr_test_pred)
        print("=========train | test=========")
        print(f"MSE: {lr_train_mse} | {lr_test_mse}")
        print(f"R2: {lr_train_r2} | {lr_test_r2}")
        print("==================")
        return None
timimg = Timing()
