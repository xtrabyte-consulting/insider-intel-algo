import pandas as pd
import numpy as np
import json
import os
from sklearn.base import BaseEstimator, TransformerMixin


class CombineFrames(BaseEstimator, TransformerMixin):
  
  def __init__(self, prices_directory: str  = '.../data/ticker-prices/compact_daily/', pricepoint: str = 'Close', insiders_file: str = '.../data/qq_insiders.csv'):
    self.prices_directory = prices_directory
    self.pricepoint = pricepoint
    self.insiders_file = insiders_file
    self.combined_df = pd.DataFrame()
  
  def get_price(self, ticker, date):
    try:
      return self.data.loc[(ticker, pd.to_datetime(date)), 'Close']
    except KeyError:
      return np.nan
    
  def format_daily_prices(self):
    
    for filename in os.listdir(self.prices_directory):
        if filename.startswith('av_query_') and filename.endswith('.csv'):
            file_path = os.path.join(self.prices_directory, filename)
            print(f'Reading {file_path}')
            ticker_info = pd.read_csv(file_path, header=None, nrows=3, skiprows=1)
            ticker = ticker_info.iloc[1, 1]  # 2nd column of the 3rd row
            
            if ticker != filename[len('av_query_'):-len('.csv')]:
              print(f'Warning: filename {filename} does not match ticker {ticker}')
              
            df = pd.read_csv(file_path, skiprows=5)
            
            # Unearth from the metadata layer
            df['Date'] = pd.to_datetime(df['5. Time Zone'])
            df.drop(columns=['US/Eastern', '5. Time Zone'], inplace=True)
            df[['Open', 'High', 'Low', 'Close', 'Volume']] = df['Unnamed: 2'].apply(lambda x: pd.Series({key: float(json.loads(x.replace("'", '"'))[f'{i}. {key.lower()}'].replace(',', '')) for i, key in enumerate(['Open', 'High', 'Low', 'Close', 'Volume'], 1)}))
            df.drop(columns=['Unnamed: 2'], inplace=True)

            # Ticker symbol column
            df['Ticker'] = ticker

            self.combined_df = pd.concat([self.combined_df, df], ignore_index=True)
  
    self.combined_df['Date'] = pd.to_datetime(self.combined_df['Date'])
    self.combined_df.set_index(['Ticker', 'Date'], inplace=True)
    self.combined_df.to_csv('agg_daily_prices.csv', index=False)
    
    return self.combined_df
  
  def format_qq_insiders(self):  

    try:
      self.data = pd.read_csv(self.insiders_file)
    except:
      raise ValueError("The file name could not be read.")
    self.data.dropna(axis=0, inplace=True)
    self.data['Date'] = pd.to_datetime(self.data['Date'])
    self.data['Ticker'] = self.data['Ticker'].astype(str)
    self.data['Name'] = self.data['Name'].astype(str).str.lower()
    self.data['fileDate'] = pd.to_datetime(self.data['fileDate'])
    self.data.drop(columns=['Unnamed: 0'], inplace=True)
    return self.data

  def fit(self, X, y=None):
    return self
  
  def transform(self, X, y=None):
    self.daily_prices = self.daily_prices.merge(self.qq_insiders, on=['Date', 'Ticker'], how='left')
    return self.daily_prices



class FormatData(BaseEstimator, TransformerMixin):
  
  
  def __init__(self, insiders_data: pd.DataFrame or str or None = None, prices_data: pd.DataFrame or str or None = None):
    self.insiders = insiders_data
    self.prices = prices_data
    
  def get_price(self, ticker, date):
    try:
      return self.data.loc[(ticker, pd.to_datetime(date)), 'Close']
    except KeyError:
      return np.nan
    
  def format_daily_prices(self, directory: str = '../data/ticker-prices/compact_daily/'):
    directory = '../data/ticker-prices/compact_daily/'
    combined_df = pd.DataFrame()

    for filename in os.listdir(directory):
        if filename.startswith('av_query_') and filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            print(f'Reading {file_path}')
            ticker_info = pd.read_csv(file_path, header=None, nrows=3, skiprows=1)
            ticker = ticker_info.iloc[1, 1]  # 2nd column of the 3rd row
            
            if ticker != filename[len('av_query_'):-len('.csv')]:
              print(f'Warning: filename {filename} does not match ticker {ticker}')
              
            df = pd.read_csv(file_path, skiprows=5)
            
            # Unearth from the metadata layer
            df['Date'] = pd.to_datetime(df['5. Time Zone'])
            df.drop(columns=['US/Eastern', '5. Time Zone'], inplace=True)
            df[['Open', 'High', 'Low', 'Close', 'Volume']] = df['Unnamed: 2'].apply(lambda x: pd.Series({key: float(json.loads(x.replace("'", '"'))[f'{i}. {key.lower()}'].replace(',', '')) for i, key in enumerate(['Open', 'High', 'Low', 'Close', 'Volume'], 1)}))
            df.drop(columns=['Unnamed: 2'], inplace=True)

            # Ticker symbol column
            df['Ticker'] = ticker

            combined_df = pd.concat([combined_df, df], ignore_index=True)
  
    combined_df['Date'] = pd.to_datetime(combined_df['Date'])
    combined_df.set_index(['Ticker', 'Date'], inplace=True)
    self.data = combined_df
    combined_df.to_csv('agg_daily_prices.csv', index=False)
    return self.data
  
  def format_qq_insiders(self):
    if isinstance(self.data, str):
      try:
        self.data = pd.read_csv(self.data)
      except:
        raise ValueError("The file name could not be read.")
    elif isinstance(self.data, pd.DataFrame):
      pass
    else:
        raise ValueError("The data must be a pandas DataFrame or a table filename.")
    self.data.dropna(axis=0, inplace=True)
    self.data['Date'] = pd.to_datetime(self.data['Date'])
    self.data['Ticker'] = self.data['Ticker'].astype(str)
    self.data['Name'] = self.data['Name'].astype(str).str.lower()
    self.data['fileDate'] = pd.to_datetime(self.data['fileDate'])
    self.data.drop(columns=['Unnamed: 0'], inplace=True)
    return self.data
  
  def combine_data(self):
    self.insiders['price_1_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=7)), axis=1)
    print('1 week done')
    self.insiders['price_2_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=14)), axis=1)
    print('2 week done')
    self.insiders['price_3_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=21)), axis=1)
    print('3 week done')
    self.insiders['price_4_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=28)), axis=1)
    print('4 week done')
    self.insiders['price_5_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=35)), axis=1)
    print('5 week done')
    self.insiders['price_6_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=42)), axis=1)
    print('6 week done')
    self.insiders['price_7_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=49)), axis=1)
    print('7 week done')
    self.insiders['price_8_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=56)), axis=1)
    print('8 week done')
    self.insiders['price_9_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=63)), axis=1)
    print('9 week done')
    self.insiders['price_10_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=70)), axis=1)
    print('10 week done')
    self.insiders['price_11_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=77)), axis=1)
    print('11 week done')
    self.insiders['price_12_week'] = self.insiders.apply(lambda x: self.get_price(x['Ticker'], x['Date'] + pd.Timedelta(days=84)), axis=1)
    print('12 week done')
    self.insiders.to_csv('full_insiders_with_prices.csv', index=False)
    return self.data
  
  def fit(self, X, y=None):
    return self
  
  def transform(self, X, y=None):
    self.format_daily_prices()
    self.format_qq_insiders()
    return self.data
  
  