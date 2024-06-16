import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
from datetime import datetime, timedelta

class FactorPlotting(object):
    __slots__ = ("close", "open", "volume", "close_path", "open_path", "volume_path")

    def __init__(self):
        self.close = None
        self.open = None
        self.volume = None
        self.close_path = "db/tw/Adj_close.parquet"
        self.open_path = "db/tw/Open.parquet"
        self.volume_path = "db/tw/Open.parquet"
        self.open = pd.read_parquet(self.open_path)
        self.close = pd.read_parquet(self.close_path)

    def get_basic_weighting(self, factor_path: str, shift: int = -2):
        '''
        factor_path : csv by default
        shift       : -2 for close_to_close factoring and close_to_close pct
                    generate factor, and trade at next trading day's close price
        '''
        factor = pd.read_csv(factor_path, index_col=[0])
        factor.index = pd.to_datetime(factor.index)
        time_interval = self.time_interval_identifier(factor_path=factor_path)
        close_price_m = self.close.resample(time_interval).last()
        close_m = close_price_m.loc[factor.index[0]:factor.index[-1]]

        if not factor.index.equals(close_m.index):
            print("index not matched error...")
            return None
        pct_m = close_m.pct_change(fill_method=None).shift(shift)
        pct_m = pct_m[pct_m.columns.intersection(factor.columns)]
        weighting_m = self.get_demean_weighting(factor)
        return weighting_m, pct_m
    def backtester(self, factor_path: str, shift: int=-2):
        weighting_m, pct_m = self.get_basic_weighting(factor_path, shift)
        weighting_m_long  = weighting_m[weighting_m > 0].fillna(0) * 2
        weighting_m_short = weighting_m[weighting_m < 0].fillna(0) * 2
        # =================================================================
        weighting_m_long_equal = self.weighting_top10(weighting_m_long, weight_method="equal")
        weighting_m_long_origin = self.weighting_top10(weighting_m_long, weight_method="origin")
        return_series_origin = (weighting_m_long_origin*pct_m).sum(axis=1)
        return_series_equal = (weighting_m_long_equal*pct_m).sum(axis=1)
        # =================================================================
        return_series = (weighting_m*pct_m).sum(axis=1)
        return_series_long = (weighting_m_long*pct_m).sum(axis=1)
        return_series_short = (weighting_m_short*pct_m).sum(axis=1)
        mdd_series = self.get_return_analysis(returns=return_series_equal)
        weighting_df = pd.DataFrame({
            "long_short": return_series.cumsum(),
            "long"      : return_series_long.cumsum(),
            "short"     : return_series_short.cumsum(),
            "origin"     : return_series_origin.cumsum(),
            "equal"     : return_series_equal.cumsum(),
            "mdd equal" : mdd_series # equal
            })
        weighting_df.plot(subplots=True, figsize=(8, 10), title='Return', grid=True)
        plt.show()
        return None
    
    def time_interval_identifier(self, factor_path: str):
        '''
        default: csv
        '''
        factor = pd.read_csv(factor_path, index_col=[0])
        index = list(factor.index)
        if isinstance(index[0], str):
            i0 = pd.to_datetime(index[0])
            i1 = pd.to_datetime(index[1])
        else:
            i0 = index[0]
            i1 = index[1]
        days = int((i1 - i0).days)
        if days <= 4:
            return "D"
        elif days >= 5 and days <= 10:
            return "W"

        elif days >= 21:
            return "ME"
        else:
            return 0

    def get_demean_weighting(self, factor: pd.DataFrame):
        df1 = factor.dropna(axis='columns', how='all').copy()
        demean = df1.sub(df1.mean(axis=1), axis=0)
        weighting = demean.div(demean.abs().sum(axis=1), axis=0)
        return weighting
    
    def weighting_top10(self, weighting: pd.DataFrame, weight_method='equal'):
        if weight_method == 'equal':
            new_weighting = weighting.apply(self._top10, axis=1)
            new_weighting = new_weighting.apply(self._reweighting_equal, axis=1)
        else:
            new_weighting = weighting.apply(self._top10, axis=1)
            new_weighting = new_weighting.apply(self._reweighting_origin, axis=1)

        return new_weighting
    
    def _top10(self, row):
        top10_indices = row.nlargest(10).index
        new_row = pd.Series(0.0, index=row.index)
        new_row[top10_indices] = row[top10_indices]
        return new_row
    
    def _reweighting_equal(self, row):
        top3_indices = row[row > 0].index
        new_row = pd.Series(0.0, index=row.index)
        new_row[top3_indices] = 0.1
        return new_row
    def _reweighting_origin(self, row):
        top3_indices = row[row > 0].index
        new_row = pd.Series(0.0, index=row.index)
        new_row[top3_indices] = row[top3_indices]/sum(row)
        return new_row
    def get_return_analysis(self, returns: pd.Series):
        '''
        returns: cunsum()/ cumprod()
        '''
        risk_free_rate = 0.03
        year_total = 4
        returns_cumsum = returns.cumsum()
        # sharpe
        sharpe = (np.mean(returns_cumsum.iloc[-1] - risk_free_rate)/ np.std(returns_cumsum))**(1/year_total)
        # karma
        positive_returns = returns[returns > 0]
        negative_returns = returns[returns < 0]
        if len(negative_returns) == 0:
            karma_ratio = np.mean(positive_returns) / 0.00001  # Avoid division by zero
        else:
            karma_ratio = np.mean(positive_returns) / np.abs(np.mean(negative_returns))
        # mdd
        rolling_max = returns_cumsum.cummax()
        drawdowns = (returns_cumsum - rolling_max) / rolling_max
        max_drawdown = drawdowns.max()
        # CAGR
        CAGR = np.round(((1 + returns_cumsum.iloc[-1])**(1/year_total) - 1)*100, 2)
        print("=====================")
        print(f"Maximum Drawdown (MDD): {np.round(-max_drawdown, 2)}%")
        print("Sharpe Ratio:", np.round(sharpe, 2))
        print(f"CAGR: {CAGR}%")
        print("Karma Ratio:", np.round(karma_ratio, 2))
        print("=====================")
        return -drawdowns

if __name__ == "__main__":
    a = FactorPlotting()
    path = 'factor/data/ARIMA_freqM_last100_2020.csv'
    # a.auto_regression(path)
    # time_interval = a.time_interval_identifier(path)
    # print(time_interval)
    a.backtester(path)

    # weighting_m, pct_m = a.get_basic_weighting(path, shift=-2)
    # weighting_m_long  = weighting_m[weighting_m > 0].fillna(0) * 2
    # # =================================================================
    # weighting_m_long_equal = a.weighting_top10(weighting_m_long, weight_method="equal")
    # return_series_equal = (weighting_m_long_equal*pct_m).sum(axis=1)
    # # =================================================================
    # mdd_series = a.get_return_analysis(returns=return_series_equal)
    # plt.plot(mdd_series)
    # plt.show()    