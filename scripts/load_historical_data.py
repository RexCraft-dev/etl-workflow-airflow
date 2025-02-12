"""
Stock Data Downloader and Database Updater

This script fetches historical stock price data for selected tickers using Yahoo Finance (yfinance),
processes the data, and inserts it into a PostgreSQL database.

Functionality:
- Downloads historical stock data from 2010-01-01 to 2024-12-31 for AAPL, MSFT, NVDA, GOOGL, and AMZN.
- Calculates the Volume Weighted Average Price (VWAP).
- Formats the data to match the PostgreSQL database schema.
- Inserts the processed data into the 'stocks' table in a PostgreSQL database.

** Make sure the PostgreSQL database 'stocks' and the appropriate table exist before running the script.
"""

import psycopg2 as pg2
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
import os

load_dotenv()

db = os.getenv('DBNAME')
user = os.getenv('USERNAME')
passwd = os.getenv('PASSWORD')

conn = pg2.connect(database=db, user=user, password=passwd)
cur = conn.cursor()

tickers = ("AAPL", "MSFT", "NVDA", "GOOGL", "AMZN")

data = pd.DataFrame(columns=['date', 'ticker', 'close', 'high', 'low', 'open', 'volume', 'vwap'])

for ticker in tickers:
    print(f'Download data for {ticker}...')
    df = yf.download(ticker, start="2010-01-01", end="2024-12-31").reset_index()

    df['date'] = df['Date']
    df['ticker'] = ticker
    df[['close', 'high', 'low', 'open', 'volume']] = df[['Close', 'High', 'Low', 'Open', 'Volume']]
    df['vwap'] = (df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum()

    df.drop(columns=['Date', 'Close', 'High', 'Low', 'Open', 'Volume'], inplace=True, level=0)

    df.columns = df.columns.droplevel(1)

    data = pd.concat([data, df])

for row in data.values:
    query = "INSERT INTO stocks (date, ticker, open, high, low, close, volume, vwap) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"

    cur.execute(query, row)

conn.commit()
print('Database updated successfully.')

cur.close()