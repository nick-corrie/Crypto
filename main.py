import sys
import pymysql
import yfinance as yf
import pandas as pd
import logging

import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Connect to our MySQL DB
try:
    engine = create_engine('MyConnectionString')
except Exception as e:
    print(e)
    sys.exit()

    
def query_data():
    """
    This function queries minutely price data from YahooFinance for the supplied stock and currency tickers.
    """
    stock_tickers = "SPY DIA NDAQ GLD SLV"            
    currency_tickers = "JPY=X GBPUSD=X AUDUSD=X NZDUSD=X CNY=X HKD=X SGD=X MXN=X"

    ticker_list = [stock_tickers, currency_tickers]
    final_df = pd.DataFrame()
    for tickers in ticker_list:
        raw_data = yf.download(tickers = tickers,
                               period = '1d',
                               interval = '1m',  
                               group_by = 'ticker')

        unstacked_data = raw_data.stack(level=0,dropna = False).reset_index()

        cleaned_data = unstacked_data.fillna(0).rename(columns = {'level_1' : 'Ticker', 'Adj Close': 'AdjClose'})
        cleaned_data['Datetime'] = cleaned_data['Datetime'].astype('str')
        final_df = final_df.append(cleaned_data)
        
    return final_df.sort_values('Datetime').reset_index(drop = True)

def my_handler(event, context):
    """
    This function queries the data and then uploads to a MySQL RDS instance.
    """
    equity_data = query_data()

    length = equity_data.shape[0]
    cols = "`,`".join([str(i) for i in equity_data.columns.tolist()])
    try:
        equity_data.to_sql('EQUITIES', con=engine, if_exists='append',index=False)
        LOGGER.info({'message' : 'added data', 'number of rows':length})
    except Exception as e:
        log_msg = {'message':'Error', 'error' : e}
        LOGGER.error(log_msg)
        
    return True
