import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
from datetime import datetime, timedelta

class FactorAnalysis(object):
    __slots__ = ("pct_close", "pct_open", "pct_close_w", "pct_open_w")

    def __init__(self):
        self.pct_close = pd.read_parquet("db/tw/pdata/close_pct.parquet")
        self.pct_close.index = pd.to_datetime(self.pct_close.index)
        # 可以用在當天收盤/隔天開盤/隔天收盤
        self.pct_open = pd.read_parquet("db/tw/pdata/open_pct.parquet")
        self.pct_open.index = pd.to_datetime(self.pct_open.index)

        self.pct_close_w = self.pct_close.resample("W-FRI").sum()
        self.pct_open_w = self.pct_open.resample("W-FRI").sum()

    def plot_basic(self, factor: pd.DataFrame, top=3, type_="cumsum"):
        list_weighting = self.get_weightings(factor, top=top)
        pct_corres = self.pct_close_w.loc[list_weighting['ls'].index[0]:].shift(-1)
        if type_ == "cumsum":
            for key, weight in list_weighting.items():
                list_weighting[key] = ((pct_corres * weight).sum(axis=1).cumsum())
        else:
            for key, weight in list_weighting.items():
                list_weighting[key] = (1 + (pct_corres * weight).sum(axis=1)).cumprod()-1
        df = pd.DataFrame(list_weighting)
        df.plot(subplots=False, label=df.columns)
        plt.show()
        return df
    def get_weightings(self, factor: pd.DataFrame, top):
        df1 = factor.dropna(axis='columns', how='all').copy()
        demean = df1.sub(df1.mean(axis=1), axis=0)
        weighting               = demean.div(demean.abs().sum(axis=1), axis=0)
        row, col = weighting.shape
        long_only_weighting     = weighting[weighting > 0].fillna(0.0)*2
        equal_weight_weighting  = pd.DataFrame(np.ones((row, col))/col, columns=weighting.columns, index=weighting.index)
        top10                   = self._topN(weighting, top=top)
        top10_equal             = self._reweighting_equal(top10)
        top10_factor_weighted   = self._reweighting_origin(top10)
        return {'ls': weighting, 
                'long': long_only_weighting,
                "equal":equal_weight_weighting,
                f"top{top}_equal": top10_equal,
                f"top{top}_factor_weighted": top10_factor_weighted,
                }

    def _topN(self, weighting:pd.DataFrame, top: int):
        def top_n_values(row: pd.Series):
            top_indices = row.nlargest(top).index
            row[~row.index.isin(top_indices)] = 0
            return row
        return weighting.apply(top_n_values, axis=1)
    
    def _reweighting_equal(self, weighting:pd.DataFrame):
        def equal_weight(row: pd.Series):
            count_larger_than_zero = (row > 0).sum()
            if count_larger_than_zero > 0:
                row = row.apply(lambda x: 1 / count_larger_than_zero if x > 0 else x)
            return row
        return weighting.apply(equal_weight, axis=1)
    
    def _reweighting_origin(self, weighting: pd.DataFrame):
        def equal_weight(row: pd.Series):
            count_larger_than_zero = row[row > 0].sum()
            if count_larger_than_zero > 0:
                row = row.apply(lambda x: x / count_larger_than_zero if x > 0 else x)
            return row
        return weighting.apply(equal_weight, axis=1)
    
    # get analysis given weighting
    # def get_return_analysis(self, returns: pd.Series):
    #     '''
    #     returns: cunsum()/ cumprod()
    #     '''
    #     risk_free_rate = 0.03
    #     year_total = 4
    #     returns_cumsum = returns
    #     # sharpe
    #     sharpe = (np.mean(returns_cumsum.iloc[-1] - risk_free_rate)/ np.std(returns_cumsum))**(1/year_total)
    #     # karma
    #     positive_returns = returns[returns > 0]
    #     negative_returns = returns[returns < 0]
    #     if len(negative_returns) == 0:
    #         karma_ratio = np.mean(positive_returns) / 0.00001  # Avoid division by zero
    #     else:
    #         karma_ratio = np.mean(positive_returns) / np.abs(np.mean(negative_returns))
    #     # mdd
    #     rolling_max = returns_cumsum.cummax()
    #     drawdowns = (returns_cumsum - rolling_max) / rolling_max
    #     max_drawdown = drawdowns.max()
    #     # CAGR
    #     CAGR = np.round(((1 + returns_cumsum.iloc[-1])**(1/year_total) - 1)*100, 2)
    #     print("=====================")
    #     print(f"Maximum Drawdown (MDD): {np.round(-max_drawdown, 2)}%")
    #     print("Sharpe Ratio:", np.round(sharpe, 2))
    #     print(f"CAGR: {CAGR}%")
    #     print("Karma Ratio:", np.round(karma_ratio, 2))
    #     print("=====================")
    #     return -drawdowns