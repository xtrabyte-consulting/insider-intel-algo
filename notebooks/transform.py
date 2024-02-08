import pandas as pd
import numpy as np
import os
import json
from sklearn.base import BaseEstimator, TransformerMixin

class NewAttributeCreator(BaseEstimator, TransformerMixin):
  
  def __init__(self, remove_zero_shares: bool = True, categorize_movement: bool = False, normalize_movemnt: bool = False, groupby: list = ['Ticker', 'Date', 'Name', 'TransactionCode', 'AcquiredDisposedCode'], format_frame: bool = True):
    self.remove_zero_shares = remove_zero_shares
    self.categorize = categorize_movement
    self.normalize = normalize_movemnt
    self.groupby = groupby
    self.format_frame = format_frame
    self.data = pd.DataFrame()
  
  def fit(self, X: pd.DataFrame, y=None):
    self.data = X
    if self.format_frame:
      self.data.dropna(axis=0, inplace=True)
      self.data['Date'] = pd.to_datetime(self.data['Date'])
      self.data['Ticker'] = self.data['Ticker'].astype(str)
      self.data['Name'] = self.data['Name'].astype(str).str.lower()
      self.data['fileDate'] = pd.to_datetime(self.data['fileDate'])
    return self.data
  
  def transform(self, X, y=None):
    if self.remove_zero_shares:
      self.data = X[X['Shares'] != 0 and X['PricePerShare'] != 0]
    self.data['total_value'] = self.data['Shares'] * self.data['PricePerShare']
    self.data['change_in_holdings'] = (self.data['Shares'] / (x['Shares'] + self.data['SharesOwnedFollowing']))
    self.data['individual_transactions_per_trade'] = self.data.groupby(['Name', 'Ticker'])['Name'].transform('count')
    self.data['investors_per_trade'] = self.data.groupby(['TransactionCode', 'Ticker'])['Name'].transform('nunique')
    self.data['TraderFrequency'] = self.data.groupby('Name')['Name'].transform('count')

    return self.data