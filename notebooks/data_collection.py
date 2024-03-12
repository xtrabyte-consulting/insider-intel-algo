import pandas as pd
import numpy as np
import requests as req
import quiverquant as qq
import sys
import os
import glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def read_api_key(filename: str) -> str:
  try:
    with open(filename, 'r') as f:
      return f.read().strip()
  except FileNotFoundError:
    print(f'File {filename} not found.')
    return None
  
def make_qq_header(api_key: str) -> dict:
  return {'Accept': 'application/json',
          'Authorization': f'Bearer {api_key}'}
  
def read_ticker_file(filename: str) -> list:
  try:
    with open(filename, 'r') as f:
      return f.read().strip().split('\n')
  except FileNotFoundError:
    print(f'File {filename} not found.')
    return None
  
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
    response = req.get(self.base_url + self.extension, headers=self.headers, params=self.params)
    if response.status_code != 200:
      raise Exception(f'Error: {response.status_code}. Failed to fetch dataset: {response.text}')
    data = response.json()
    
    if isinstance(data, dict):
      if 'Error Message' in data.keys():
        raise Exception(f'JSON Error Message: {data["Error Message"]}')
      print(f'JSON Keys: {data.keys()}')
      
    df = pd.DataFrame(response.json())
    return df
      
  def __download_datasets(self, param: str, values: list) -> [pd.DataFrame]:
    dfs = []
    
    for value in values:
      try:
        self.params[param] = value
        print("Params: {}".format(self.params))
        df= self.__get_single_dataset()
        if glob.glob(f'{self.DATA_FILEPATH}{self.api_name}_{self.extension.replace("/", "-")}_{value}\.csv'):
          df = pd.concat([pd.read_csv(f) for f in glob.glob(f'{self.DATA_FILEPATH}{self.api_name}_{self.extension.replace("/", "-")}_{value}\.csv')])
        df.to_csv(f'{self.DATA_FILEPATH}{self.api_name}_{self.extension.replace("/", "-")}_{value}.csv', index=True)
        dfs.append(df)
      except Exception as e:
        print(f'Error: {e}')
        continue
      
    return dfs
  
  def download_datasets(self, param: str, values: list) -> [pd.DataFrame]:
    with ThreadPoolExecutor() as executor:
      return executor.submit(self.__download_datasets, param, values).result()
      
  def download_dataset(self) -> pd.DataFrame:
    with ThreadPoolExecutor(max_workers=10) as executor:
      df = executor.submit(self.__get_single_dataset).result()
      if self.extension is not None:
        if glob.glob(f'{self.DATA_FILEPATH}{self.api_name}_{self.extension.replace("/", "-")}.csv'):
          df = pd.concat([pd.read_csv(f) for f in glob.glob(f'{self.DATA_FILEPATH}{self.api_name}_{self.extension.replace("/", "-")}.csv')])
        df.to_csv(f'{self.DATA_FILEPATH}{self.api_name}_{self.extension.replace("/", "-")}.csv', index=True)
        return df
  
class QuiverDatasets:
  
  #SP500_TICKERS = pd.read_csv('https://datahub.io/core/s-and-p-500-companies/r/constituents.csv').Symbol.tolist()
  QQ_BASE_URL = 'https://api.quiverquant.com/'
  
  def __init__(self, api_key = read_api_key('keys/qq_api_key.txt'), base_url = QQ_BASE_URL, tickers = []):
    self.api_key = api_key
    self.base_url = base_url
    self.tickers = tickers
    self.header = make_qq_header(api_key)
    
  def get_live_insider_set(self):
    # Get the insider trading data from QuiverQuant
    header = {'Accept': 'application/json',
                'Authorization': f'Bearer {self.api_key}'}
    extension = 'beta/live/insiders'
    params = {'limit_codes': 'true'}
    importer = ImportData(self.base_url, 'qq', header, params, extension)
    return importer.download_dataset()
  
class SecApiIO:
  SAIO_BASE_URL = 'https://api.sec-api.io/'
  f = 'https://api.sec-api.io/insider-trading?limit=100&sort=-transaction_date&filter=%7B%22transaction_date%22%3A%7B%22gt%22%3A%222021-01-01%22%7D%7D'


import sched
import time

class AlphaVantageDatasets:
    
  AV_BASE_URL = 'https://www.alphavantage.co/'
  AV_API_KEY = read_api_key('keys/av_api_key.txt')
  ABOVE_MEDIAN_TICKERS = read_ticker_file('outputs/tickers_value_over_median.txt')
  
  def __init__(self, api_key = AV_API_KEY, base_url = AV_BASE_URL):
    self.api_key = api_key
    self.base_url = base_url
    self.header = {'Accept': 'application/json'}
    self.extension = 'query'
    self.params = {'apikey': self.api_key}
    self.series_frame = pd.DataFrame()
    
  def get_daily(self, ticker: str, outputsize: str = 'full') -> pd.DataFrame:
    # Get the daily data from AlphaVantage
    params = {'function': 'TIME_SERIES_DAILY',
              'symbol': ticker,
              'outputsize': outputsize,
              'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params, self.extension)
    return importer.download_dataset()
  
  def __get_daily_batch(self, start: int, tickers: list, scheduler: sched.scheduler, outputsize: str = 'full') -> None:

    if start + 30 > len(tickers) - 1:
      print("Downloading final tickers {} to {}".format(start, len(tickers)-1))
      t_30 = tickers[start:]
      finished = True
    else:
      t_30 = tickers[start:start+25]
      print("Downloading tickers {} to {}".format(tickers.index(t_30[0]), tickers.index(t_30[-1])))
      finished = False

    # Get the daily timeseries data from AlphaVantage
    params = {'function': 'TIME_SERIES_DAILY',
          'outputsize': outputsize,
          'apikey': self.api_key}
    importer = ImportData(self.base_url, 'av', self.header, params=params, extension=self.extension)
    frames = importer.download_datasets('symbol', t_30)
    frame = pd.concat(frames)
    print("Downloaded frame size: {}".format(frame.shape))
    self.series_frame = pd.concat([self.series_frame, frame])
    print("Series frame size: {}".format(self.series_frame.shape))

    if finished:
      self.series_frame.to_csv(f'../data/ticker-prices/av_agg_daily_series.csv', index=True)
      return
    else:
      scheduler.enter(70, 1, self.__get_daily_batch, (start+25, tickers, scheduler, outputsize))
    
  def get_daily_batch(self, tickers: list = ABOVE_MEDIAN_TICKERS, outputsize: str = 'compact') -> pd.DataFrame:
    """
    Downloads daily series of ticker data from AlphaVantage API with the 30 call/min limit.

    Args:
      start (int): The starting index of the tickers list.
      tickers (list): List of ticker symbols.
      outputsize (str, optional): The size of the output data. Options are 'compact' or 'full.
                                  Defaults to 'full'.

    Returns:
      pd.DataFrame: The dataframe of the daily series data for all tickers.

    Raises:
      Exception: If the response status code is not 200.
    """
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(0, 1, self.__get_daily_batch, (0, tickers, scheduler, outputsize))
    scheduler.run()
    return self.series_frame
    
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
  
if __name__ == '__main__':
  # Get the insider trading data from QuiverQuant
  qq = QuiverDatasets()
  insiders = qq.get_live_insider_set()
  print(insiders.head())
  print(insiders.shape)
  print(insiders.tail())
  print(insiders.Ticker.value_counts())
  print(insiders.Name.value_counts())
  tickers = insiders.Ticker.value_counts().index.tolist()
  # Get the daily timeseries data from AlphaVantage
  av = AlphaVantageDatasets()
  #with open('outputs/missing_tickers.txt', 'r') as f:
  # tickers = f.read().split('\n')
  print(f'Starting download of {len(tickers)} Ticker price sets...')
  df = av.get_daily_batch(tickers, outputsize='compact')
  df.to_csv(f'../data/ticker-prices/av_agg_daily.csv', index=True)
  print(f'Frame downloaded {df.shape}')
  # Get the daily adjusted data from AlphaVantage
  #av.get_daily_adjusted_batch(tickers)
  # Get the company overview data from AlphaVantage
  #av.get_company_overview_batch(tickers)
  # Get the income statement data from AlphaVantage
  #av.get_income_statement_batch(tickers)
  # Get the ema data from AlphaVantage
  #av.get_emas_batch(tickers)
  # Get the sma data from AlphaVantage
  #av.get_sma_batch(tickers)
  # Get the rsi data from AlphaVantage
  #av.get_rsi_batch(tickers)
  # Get the bbands data from AlphaVantage
  #av.get_bbands_batch(tickers)