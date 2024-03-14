from sklearn.base import BaseEstimator, TransformerMixin



class CreateAttributes(BaseEstimator, TransformerMixin):
    
    def __init__(self, df: pd.DataFrame):
      self.df = df
      
    def fit(self, df: pd.DataFrame, y=None) -> 'CreateAttributes':
      return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
      df['value_cat'] = pd.qcut(df['total_value'], q=10, labels=False, duplicates='drop')
      df = df.drop('fileDate', axis=1)
      insiders['change_in_holdings'] = (insiders['Shares'] / insiders['SharesOwnedFollowing']) * 1
      df = df.groupby(['Ticker', 'Date', 'Name', 'TransactionCode', 'AcquiredDisposedCode']).agg({'Shares': 'sum', 'PricePerShare': 'mean', 'SharesOwnedFollowing': 'min', 'total_value': 'sum', 'TraderFrequency': 'mean', 'change_in_holdings': 'sum', 'individual_transactions_per_trade': 'first', 'investors_per_trade': 'first', 'price_2_week': 'mean', 'price_3_week': 'mean', 'price_4_week': 'mean', 'price_5_week': 'mean', 'price_6_week': 'mean', 'price_7_week': 'mean', 'price_8_week': 'mean', 'price_9_week': 'mean', 'price_10_week': 'mean', 'price_11_week': 'mean', 'price_12_week': 'mean'}) 
      insiders['individual_transactions_per_trade'] = insiders.groupby(['Name', 'Ticker'])['Name'].transform('count')
insiders['investors_per_trade'] = insiders.groupby(['TransactionCode', 'Ticker'])['Name'].transform('nunique')
      return df