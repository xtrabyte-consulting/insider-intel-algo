import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from data_collection import QuiverDatasets

class InsiderSetAnalysis:
  
  def __init__(self, insiders: pd.DataFrame):
    self.insiders = insiders
    
  def __visualize_ticker_freq(self) -> None:
    # Visualize the data
    sns.histplot(data=self.insiders, x=self.insiders.Ticker.value_counts())
    
  def analyze_data(self) -> None:
    self.__visualize_ticker_freq()
    
  def generate_stats(self, suffix:str = '') -> None:
    with open('insider_stats.txt', 'w') as f:
      sys.stdout = f
    print(self.insiders.describe())
    print(self.insiders.info())
    print(self.insiders.head())
    print(self.insiders.tail())
    print(self.insiders.shape)
    print(self.insiders.Ticker.value_counts())
    print(self.insiders.Name.value_counts())
    sys.stdout = sys.__stdout__
    

class PreprocessingInsiderSet:
  
  def __init__(self, df: pd.DataFrame):
    self.insidersdf = df
    
  def __visualize_ticker_freq(self) -> None:
    # Visualize the data
    sns.histplot(data=self.insiders, x=self.insiders.Ticker.value_counts())
    
  def analyze_data(self) -> None:
    self.__visualize_ticker_freq()
    
  def generate_stats(self, suffix:str = '') -> None:
    with open('insider_stats.txt', 'w') as f:
      sys.stdout = f
    print(self.insiders.describe())
    print(self.insiders.info())
    print(self.insiders.head())
    print(self.insiders.tail())
    print(self.insiders.shape)
    print(self.insiders.Ticker.value_counts())
    print(self.insiders.Name.value_counts())
    sys.stdout = sys.__stdout__
    
  def __clean_data(self) -> pd.DataFrame:
    # Drop rows with missing values
    self.df.dropna(axis=0, inplace=True)
    self.df.drop(self.df[self.df['Shares'] == 0].index, inplace=True)
    self.df.drop(self.df[self.df['PricePerShare'] == 0].index, inplace=True)
    self.df['Date'] = pd.to_datetime(self.df['Date'])
    self.df['Ticker'] = self.df['Ticker'].astype(str)
    self.df['Name'] = self.df['Name'].astype(str).str.lower()
    self.df['fileDate'] = pd.to_datetime(self.df['fileDate'])
    return self.df
  
  def __generate_features(self) -> pd.DataFrame:
    # Generate features
    self.df['Total'] = self.df['Shares'] * self.df['PricePerShare']
    self.df['HoldingsPercent'] = (self.df['Shares'] / self.df['SharesOwnedFollowing']) * 100
    self.df['TraderFrequency'] = self.df.groupby('Name')['Name'].transform('count')
    return self.df
  
  def __normalize_data(self) -> pd.DataFrame:
    # Normalize the data
    self.df = (self.df - self.df.mean()) / self.df.std()
    return self.df
  
  def __visualize_data(self) -> None:
    # Visualize the data
    sns.set()
    self.df.plot(figsize=(10, 5))
    plt.ylabel('Price')
    plt.title('Stock Price')
    plt.show()
    
  def preprocess_data(self) -> pd.DataFrame:
    self.__clean_data()
    self.__normalize_data()
    self.__visualize_data()
    return self.df