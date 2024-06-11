import numpy as np
import alphalens
class AlphaLens(object):
    def __init__(self):
        print("AlphaLens initing")
    
    def alphalens_get_processed_factor(self, factor, price, quantiles, periods):
        factor_data = alphalens.utils.get_clean_factor_and_forward_returns(
            factor, 
            price, 
            quantiles=quantiles,
            periods=periods,
            bins=None,
            maxc_loss=0.35)
        return factor_data

    def alphalens_get_summary(self, factor):
        df = alphalens.tears.create_summary_tear_sheet(factor, long_short=True, group_neutral=False)
        return df
    
    def alphalens_get_ic(self, factor):
        df = alphalens.performance.factor_information_coefficient(factor, group_adjust=False, by_group=False)
        return df
    