import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class CombineFrames(BaseEstimator, TransformerMixin):
  
  def __init__(self, daily_prices: pd.DataFrame, qq_insiders: pd.DataFrame):
    self.daily_prices = daily_prices
    self.qq_insiders = qq_insiders
  
  def fit(self, X, y=None):
    return self
  
  def transform(self, X, y=None):
    self.daily_prices = self.daily_prices.merge(self.qq_insiders, on=['Date', 'Ticker'], how='left')
    return self.daily_prices