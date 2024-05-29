# Alpha DB
# https://spring-reading-ca7.notion.site/b0061145e3414afe8de3d0a4137a467c

import os, sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, PROJECT_ROOT)

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from backtest.inventory import AlphaInventory

from computation.operators import *
from computation.factor_analysis_tool import *
from computation.performance_generator import *
from utils.path_helper import *
from utils.hint_texter import *

class FactorDevJimmy(AlphaInventory):

    def __init__(self, load_all_factor=False, load_prepared_data=True, alpha_data_path=alpha_data_path):
        super().__init__(alpha_data_path=alpha_data_path, load_prepared_data=load_prepared_data)

        self.pmart_columns = pq.ParquetFile(f'{self.alpha_data_path}/pmart.parquet').schema.names       
        self.factor_required_data = None

        if load_prepared_data:
            print('Start loading factor prepared data...')
            self.run_existing_factor_required_data()

        # alpha inventory
        self.operating_income_to_cap = None
        self.time_series_ar_rolling = None
        self.broker_based_individual_factor = None
        self.log_total_assets = None
        self.log_market_value = None
        self.return_on_sales = None

        if load_all_factor:
            self.calculate_and_get_factor_dict()
        else:
            print(alpha_class_init_hint_text)

    def calculate_and_get_factor_dict(self):
        print('Getting all factors...')
        print('|-------------------------------------------------|')
        self.get_roa_to_cap()
        self.get_operating_income_to_cap()
        print('|-------------------------------------------------|')

        self.factor_dict = {
            # 1th
            'operating_income_to_cap': self.operating_income_to_cap,
        }
        self.factor_all = self.combine_factor(self.factor_dict)
    
    # ==================== Ted's factors reproduction ====================
    # Name: 營業利益回報
    def get_operating_income_to_cap(self):
        if self.operating_income_to_cap is None:
            print('Getting Alpha: operating_income_to_cap')
            pmart = pd.read_parquet(os.path.join(alpha_data_path, "pmart.parquet"))
            outstanding, operating_margin, close, revenue_m = '流通股數(千股)', '營業利益', "收盤價-除權息", '單月營收成長率%'
            factor_longformat = pmart[[outstanding, operating_margin, close, revenue_m, "datetime", "symbol"]]
            factor_longformat.dropna(inplace=True)
            factor_longformat["numerator"]   = factor_longformat[revenue_m] * factor_longformat[operating_margin]
            factor_longformat["denominator"] = factor_longformat[close]     * factor_longformat[outstanding]
            factor_longformat['factor'] = factor_longformat["numerator"] / factor_longformat['denominator']

            factor_pivoted = factor_longformat.pivot(index="datetime", columns="symbol", values='factor')

            factor = self.get_demean_weighting(factor_pivoted)
            self.operating_income_to_cap = factor
        return self.operating_income_to_cap

    # Name: log(資產)
    def get_log_total_assets(self):
        if self.log_total_assets is None:
            print("Getting Alpha: log_total_assets")
            self.log_total_assets = ts_rank((self.conduct_industry_neutral(self.remove_outlier(np.log(self.factor_required_data['資產總額']).unstack()))), 252)
        return self.log_total_assets

    # Name: log(流動現價總額)
    def get_log_market_value(self):
        if self.log_market_value is None:
            print("Getting Alpha: log_market_value")
            close              = self.factor_required_data(['收盤價-除權息'])
            shares_outstanding = self.factor_required_data(['流通股數(千股)'])
            self.log_market_value = ts_rank((self.conduct_industry_neutral(self.remove_outlier(np.log(close*shares_outstanding).unstack()))), 252)
        return self.log_market_value
    
    def get_return_on_sales(self):
        if self.return_on_sales is None:
            print("Getting Alpha: return_on_sales")
            close                = self.factor_required_data(['收盤價-除權息'])
            shares_outstanding   = self.factor_required_data(['流通股數(千股)'])
            revenue_m            = self.factor_required_data(['單月營收(千元)'])
            factor               = (revenue_m/(close*shares_outstanding)).unstack()
            self.return_on_sales = ts_rank((self.conduct_industry_neutral(self.remove_outlier(factor))), 252)
        return self.return_on_sales
    # ==================== Alpha List ====================
    
    def get_time_series_ar_rolling(self):
        if self.time_series_ar_rolling is None:
            print("Getting Alpha: time_series_ar_rolling")
            self.time_series_ar_rolling = None
        return self.time_series_ar_rolling
        
    def get_broker_based_individual_factor(self):
        if self.broker_based_individual_factor in None:
            print("Getting Alpha: broker_based_individual_factor")
            self.broker_based_individual_factor = None
        return self.broker_based_individual_factor
    
    def example(self):
        if self.revenue_growth_momentum is None:
            print('Getting Alpha: revenue_growth_momentum')
            self.revenue_growth_momentum = ts_rank((self.conduct_industry_neutral(self.remove_outlier(self.factor_required_data['單月營收成長率%'].unstack()))), 252)
        return self.revenue_growth_momentum