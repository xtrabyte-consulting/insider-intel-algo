import pandas as pd
import numpy as np
import requests as req
import quiverquant as qq
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def read_api_key(filename: str) -> str:
  try:
    with open(filename, 'r') as f:
      return f.read().strip()
  except FileNotFoundError:
    print(f'File {filename} not found.')
    return None
  
def make_qq_header(self, api_key: str) -> dict:
  return {'Accept': 'application/json',
          'Authorization': f'Bearer {api_key}'}
  
class ImportData:
  
  DATA_FILEPATH = '../data/'
  
  def __init__(self, base_url: str, api_name: str, headers = None, params = None, extension = ''):
    self.base_url = base_url
    self.headers = headers if headers is not None else {}
    self.params = params if params is not None else {}
    self.api_name = api_name
    self.extension = extension
    
  def __get_single_dataset(self) -> pd.DataFrame:
    # Get a single dataset from any API
    response = req.get(self.base_url + self.extension, headers=self.headers, params = self.params)
    if response.status_code != 200:
      raise Exception(f'Error: {response.status_code}. Failed to fetch dataset: {response.text}')
    df = pd.DataFrame(response.json())
    if self.extension is not None:
      df.to_csv(f'{self.api_name}_{self.extension.replace("/", "-")}', index=True)
    return df
      
  def download_datasets(self, param: str, values: list) -> [pd.DataFrame]:
    dfs = []
    
    def get_parameter_dataset(value: str) -> pd.DataFrame:
    # Get dataset with specified param from any API
      self.params[param] = value
      return self.__get_single_dataset(self.extension)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
      dfs = list(executor.map(get_parameter_dataset, values))
      
    return dfs
      
  def download_dataset(self) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=10) as executor:
        return executor.submit(self.__get_single_dataset).result()
  
class QuiverDatasets:
  
  SP500_TICKERS = pd.read_csv('https://datahub.io/core/s-and-p-500-companies/r/constituents.csv').Symbol.tolist()
  QQ_BASE_URL = 'https://api.quiverquant.com/'
  
  def __init__(self, api_key = read_api_key('keys/qq_api_key.txt'), base_url = QQ_BASE_URL, tickers = SP500_TICKERS):
    self.api_key = api_key
    self.base_url = base_url
    self.tickers = tickers
    self.header = make_qq_header(self.api_key)
    
  def get_live_insider_set(self):
    # Get the insider trading data from QuiverQuant
    header = {'Accept': 'application/json',
                'Authorization': f'Bearer {self.api_key}'}
    extension = 'beta/live/insiders'
    params = {'limit_codes': 'true'}
    importer = ImportData(self.base_url, 'qq', header, params, extension)
    return importer.download_dataset()

import sched
import time

class AlphaVantageDatasets:
    
  AV_BASE_URL = 'https://www.alphavantage.co/'
  AV_API_KEY = read_api_key('keys/av_api_key.txt')
  
  def __init__(self, api_key = AV_API_KEY, base_url = AV_BASE_URL):
    self.api_key = api_key
    self.base_url = base_url
    self.header = {'Accept': 'application/json'}
    self.extension = 'query'
    self.params = {'apikey': self.api_key}
    
  def get_daily_adjusted(self, ticker: str, outputsize: str = 'full') -> pd.DataFrame:
    # Get the daily adjusted data from AlphaVantage
    params = {'function': 'TIME_SERIES_DAILY_ADJUSTED',
              'symbol': ticker,
              'outputsize': outputsize,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def get_daily_adjusted_batch(self, tickers: list, outputsize: str = 'full') -> [pd.DataFrame]:
    # Get the daily adjusted data from AlphaVantage
    params = {'function': 'TIME_SERIES_DAILY_ADJUSTED',
              'outputsize': outputsize,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params=params, extension=self.extension)
    return importer.download_datasets('symbol', tickers)
  
  def get_company_overview(self, ticker: str) -> pd.DataFrame:
    # Get the company overview data from AlphaVantage
    params = {'function': 'OVERVIEW',
              'symbol': ticker,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def get_company_overview_batch(self, tickers: list) -> [pd.DataFrame]:
    # Get the company overview data from AlphaVantage
    params = {'function': 'OVERVIEW',
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params=params, extension=self.extension)
    return importer.download_datasets('symbol', tickers)
  
  def get_income_statement(self, ticker: str, period: str = 'annual') -> pd.DataFrame:
    # Get the income statement data from AlphaVantage
    params = {'function': 'INCOME_STATEMENT',
              'symbol': ticker,
              'period': period,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def get_emas(self, ticker: str, interval: str = 'daily', time_period: int = 50, series_type = 'close') -> pd.DataFrame:
    # Get the ema data from AlphaVantage
    params = {'function': 'EMA',
              'symbol': ticker,
              'interval': interval,
              'time_period': time_period,
              'series_type': series_type,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def get_sma(self, ticker: str, interval: str = 'daily', time_period: int = 50, series_type = 'close') -> pd.DataFrame:
    # Get the sma data from AlphaVantage
    params = {'function': 'SMA',
              'symbol': ticker,
              'interval': interval,
              'time_period': time_period,
              'series_type': series_type,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def get_rsi(self, ticker: str, interval: str = 'daily', time_period: int = 50, series_type = 'close') -> pd.DataFrame:
    # Get the rsi data from AlphaVantage
    params = {'function': 'RSI',
              'symbol': ticker,
              'interval': interval,
              'time_period': time_period,
              'series_type': series_type,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def get_bbands(self, ticker: str, interval: str = 'daily', time_period: int = 50, series_type = 'close') -> pd.DataFrame:
    # Get the bbands data from AlphaVantage
    params = {'function': 'BBANDS',
              'symbol': ticker,
              'interval': interval,
              'time_period': time_period,
              'series_type': series_type,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()