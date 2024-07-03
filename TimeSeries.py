import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt 
import statsmodels.api as sm
from statsmodels.tsa.stattools import pacf, acf
from statsmodels.graphics.tsaplots import plot_pacf, plot_acf
from arch import arch_model
from FactorAnalysis import FactorAnalysis
from tqdm import tqdm
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

class TimeSeries(FactorAnalysis):
    def __init__(self):
        __slots__ = ("bt_period_w")
        super().__init__()
        # 用在隔天開盤/收盤
        self.bt_period_w = 52

    def arma(self, start=0, ticker_len=10, ma_t=2):
        pct_close_w = self.pct_close_w
        indexes = pct_close_w.index
        tickers = pct_close_w.columns[start:ticker_len]
        indices = indexes[self.bt_period_w:]
        forecast = np.zeros((len(indices), len(tickers)))
        for index, date in enumerate(tqdm(indices, desc=f"arma model for top {ticker_len}, enumerate by da")):
            pct_train = pct_close_w.loc[indexes[index]:date]
            for i in range(ticker_len):
                series = pct_train[tickers[i]]
                if series.isna().any():
                    prediction = 0
                    print(tickers[i], "failed at", date)
                    continue
                    ###### ARIMA ma
                else:
                    model = sm.tsa.arima.ARIMA(series, order=(ma_t, 0, 0))
                    model_fit = model.fit()
                    prediction = model_fit.forecast(horizon=1).values[0]
                forecast[index, i] = prediction
        outcome = pd.DataFrame(forecast, columns=tickers, index=indices)
        return outcome

    def arima_garch(self):
        test_range = -250
        pct_close = self.pct_close
        test = pct_close['2330.TW'].iloc[-test_range:]
        predictions = [0.0] * 100
        for index in range(100):
            print(index)
            train_data = pct_close['2330.TW'].iloc[-test_range+index:-test_range+100+index]
            prediction = self._arima_forecast_1(train_data)
            predictions[index] = prediction
        
        print("test finished")
        return predictions, test

if __name__ == "__main__":
    a = TimeSeries()
    a.arima_garch()