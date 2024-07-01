import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt 
import statsmodels.api as sm
from statsmodels.tsa.stattools import pacf, acf
from statsmodels.graphics.tsaplots import plot_pacf, plot_acf
from arch import arch_model
from FactorPlotting import FactorPlotting

import warnings
warnings.filterwarnings("ignore")

'''
/Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages/statsmodels/tsa/base/tsa_model.py:473: ValueWarning: A date index has been provided, but it has no associated frequency information and so will be ignored when e.g. forecasting.
  self._init_dates(dates, freq)
=> wouldn't influence the outcome
=======================================

# MLE optimization not converge for ar model
# /Library/Frameworks/Python.framework/Versions/3.11/lib/python3.11/site-packages/statsmodels/regression/linear_model.py:1491: ValueWarning: Matrix is singular. Using pinv.
#   warnings.warn("Matrix is singular. Using pinv.", ValueWarning)
'''

class TimeSeries(FactorPlotting):
    def __init__(self):
        __slots__ = ("pct_change_open", "pct_change_close", "train_interval")
        super().__init__()
        ### create data main for two recession than others, set data for 2019-01-01 ~ 2023
        self.pct_change_close = self.close.pct_change(fill_method=None)
        self.pct_change_open = self.open.pct_change(fill_method=None)
        self.pct_change_close.index = pd.to_datetime(self.pct_change_close.index)
        self.pct_change_open.index = pd.to_datetime(self.pct_change_open.index)
        # 用在隔天開盤/收盤
        self.pct_change_close = self.pct_change_close.loc[pd.to_datetime('2015-01-01'):pd.to_datetime('2024-01-01')]
        # 可以用在當天收盤/隔天開盤/隔天收盤
        self.pct_change_open = self.pct_change_open.loc[pd.to_datetime('2015-01-01'):pd.to_datetime('2024-01-01')]

        self.train_interval = 200
    # default: resample("W")
    def arma(self, ma=0, i=0):
        pct_change_close = self.pct_change_close
        pct_change_close_w = pct_change_close.resample("W").sum()
        indexes = pct_change_close_w.index
        tickers = pct_change_close_w.columns[:100]
        factors = []
        indices = indexes[self.train_interval:]
        start = time.time()
        for index, date in enumerate(indices):
            pct_train = pct_change_close_w.loc[indexes[index]:date]
            prediction_factor = []
            print(indexes[index], "=>", date)
            for ticker in list(tickers):
                series = pct_train[ticker]
                if series.isna().any():
                    prediction = 0
                    print(ticker, "failed at", date)
                    continue
                    ###### ARIMA ma
                if ma == 1:
                    ma_value = acf(series)
                    N = len(ma_value)
                    threshold = 1.96/np.sqrt(N)
                    significant_indices = np.where(np.abs(ma_value) > threshold)[0]
                    if len(significant_indices) >= 2:
                        ma_t = significant_indices[1]
                    elif len(significant_indices) == 1:
                        ma_t = np.argmax(np.abs(ma_value[1:])) + 1
                else:
                    ma_t = 0
                ###### _return_best_lag
                # ma_value = pacf(series)
                # array_acf_best3 = np.argsort(abs(ma_value))[-4:-1][::-1] # is log
                # models = [sm.tsa.arima.ARIMA(series, order=(i, 0, 0)) for i in array_acf_best3]# order = (ar_t, i_t, ma_t), for ar model only => ma_t = 0
                # model_fits = [model.fit() for model in models]
                # model_aics = [model_fit.aic for model_fit in model_fits]
                # prediction = model_fits[np.argmin(model_aics)].forecast().iloc[0]
                # prediction_factor.append(prediction)
                prediction = self._arima_forecast_1(series=series, ma_t=ma_t)
                prediction_factor.append(prediction)
            factors.append(prediction_factor)
        outcome = pd.DataFrame(factors, columns=tickers, index=indices)
        outcome.to_parquet(f"factor/data/AR_MA{ma}_2019_2023_F100.parquet")
        print(time.time() - start)
        return outcome

    def _arima_forecast_1(self, series: pd.Series, ma_t: int) -> float:
        ar_value = pacf(series)
        array_acf_best3 = np.argsort(abs(ar_value))[-4:-1][::-1] # is log
        models = [sm.tsa.arima.ARIMA(series, order=(ma_t, 0, 0)) for i in array_acf_best3]# order = (ar_t, i_t, ma_t), for ar model only => ma_t = 0
        model_fits = [model.fit() for model in models]
        model_aics = [model_fit.aic for model_fit in model_fits]
        prediction = model_fits[np.argmin(model_aics)].forecast().iloc[0]
        return prediction
    # garch target on pct_change not residuals

    def garch(self) -> pd.DataFrame:
        pct_change_close = self.pct_change_close
        pct_change_close_w = pct_change_close.resample("W").sum().fillna(0)
        indexes = pct_change_close_w.index
        tickers = pct_change_close_w.columns[:100]
        factors = []
        indices = indexes[self.train_interval:]
        start = time.time()
        for index, date in enumerate(indices):
            pct_train = pct_change_close_w.loc[indexes[index]:date]
            prediction_factor = []
            print(indexes[index], "=>", date)
            for ticker in list(tickers):
                series = pct_train[ticker]
                if series.isna().any():
                    prediction = 0
                    print(ticker, "failed at", date)
                    continue
                pacf_values = pacf(series, method="ywm")
                N = len(pacf_values)
                threshold = 1.96/np.sqrt(N)
                significant_indices = np.where(np.abs(pacf_values) > threshold)[0]
                if len(significant_indices) >= 2:
                    best_lag_p = significant_indices[1]
                elif len(significant_indices) == 1:
                    best_lag_p = np.argmax(np.abs(pacf_values[1:])) + 1
                model = arch_model(series*100, p=best_lag_p, q=0)
                model_fit = model.fit(disp='off')
                prediction = model_fit.forecast(horizon=1)
                prediction_factor.append(prediction.variance.values[-1, :][0])
            factors.append(prediction_factor)
        outcome = pd.DataFrame(factors, columns=tickers, index=indices)
        outcome.to_parquet(f"factor/data/GARCH_close_2019_2023_F100.parquet")
        print("time spent:", time.time() - start)
        return outcome
    '''
    # auto regressive conditional heteroskedasticity/heteroscedasticity
    # from real values - predicted values plotting
    # test on TWSE Index should be enough for step 1
    # obstacle to be "expanding to multidimension prediction problem"
    # how to consider volume as an influencer
    '''
    def arima_garch(self):
        test_range = -250
        pct_change_close = self.pct_change_close
        test = pct_change_close['2330.TW'].iloc[-test_range:]
        predictions = [0.0] * 100
        for index in range(100):
            print(index)
            train_data = pct_change_close['2330.TW'].iloc[-test_range+index:-test_range+100+index]
            prediction = self._arima_forecast_1(train_data)
            predictions[index] = prediction
        
        print("test finished")
        return predictions, test
    def fft(self):
        pass
    def sarima(self):
        pass

if __name__ == "__main__":
    a = TimeSeries()
    # a.arma()
    # a.garch()
    a.arima_garch()