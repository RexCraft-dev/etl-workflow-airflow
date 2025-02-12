from psycopg2 import connect, sql, Error
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import sys
import logging

# Set up logging configuration
log_dir = "logs"
log_file = os.path.join(log_dir, "stocks_etl.log")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(filename=log_file, 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load database credentials and ticker symbols from environment variables
load_dotenv()
db = os.getenv('DBNAME')
table = os.getenv('TABLE_NAME')
user = os.getenv('USERNAME')
passwd = os.getenv('PASSWORD')
tickers = tuple(os.getenv('TICKERS').split(','))

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        return connect(database=db, user=user, password=passwd)
    except Exception as e:
        logging.error(f"Database connection failed: {e}", exc_info=True)
        sys.exit(1)

def extract():
    # Extract stock data from Yahoo Finance based on the last available date in the database.
    logging.info("ETL process started.")
    logging.info(f"Processing tickers: {tickers}")
    raw_data = []

    for ticker in tickers:
        try:
            # Get last available date for the ticker from the database
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    query = sql.SQL("SELECT MAX(date) FROM {} WHERE ticker = %s").format(sql.Identifier(table))
                    cur.execute(query, (ticker,))
                    start_date = cur.fetchone()[0]  

            # Define start and end date for data extraction
            start_date = (start_date + timedelta(days=1)).strftime("%Y-%m-%d") if start_date else "2010-01-01"
            end_date = datetime.today().strftime("%Y-%m-%d")

            if start_date >= end_date:
                logging.info(f'[{ticker}] No new data available.')
                continue
            
            logging.info(f'[{ticker}] Downloading data ({start_date} to {end_date})...')

            # Download stock data from Yahoo Finance
            df = yf.download(ticker, 
                             start=start_date, 
                             end=end_date, 
                             progress=False, 
                             multi_level_index=False,
                             rounding=True).reset_index()

            if df.empty:
                logging.warning(f"[{ticker}] No new data found.")
                raw_data.append(pd.DataFrame())
                continue

            raw_data.append(df)
            logging.info(f'[{ticker}] Data downloaded successfully. ({len(df)})')

        except Error as db_err:
            logging.error(f"Database query failed for [{ticker}]: {db_err}")
        except Exception as e:
            logging.error(f"Unexpected error during extraction for [{ticker}]: {e}", exc_info=True)

    if all(df.empty for df in raw_data):
        logging.info("All tickers returned empty data. Exiting script.")
        logging.info("ETL process completed.")
        sys.exit(0)

    return raw_data

def transform(raw_data):
    # Transform raw stock data by renaming columns and calculating VWAP.
    logging.info("Transforming data...")
    data_transformed = None

    for ticker, df in zip(tickers, raw_data):
        try:
            if df.empty:
                logging.info(f'[{ticker}] No data available...')
                continue

            # Add ticker symbol to the dataset
            df['ticker'] = ticker
            
            # Calculate VWAP (Volume Weighted Average Price)
            df['vwap'] = round((df['Close'] * df['Volume']).cumsum() / df['Volume'].cumsum(), 2)

            # Rename columns to match database schema
            df.rename(columns={'Date': 'date', 
                               'Close': 'close', 
                               'High': 'high', 
                               'Low': 'low', 
                               'Open': 'open', 
                               'Volume': 'volume'}, 
                               inplace=True)
            
            cols = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume', 'vwap']
            df = df[cols]

            if data_transformed is None:
                data_transformed = df
            else:
                data_transformed = pd.concat([data_transformed, df], ignore_index=True)

            logging.info(f'[{ticker}] Data transformed successfully.')

        except Exception as e:
            logging.error(f"Data transformation failed for {ticker}: {e}")

    return data_transformed

def load(transformed_data):
    # Load transformed stock data into the PostgreSQL database.
    logging.info("Loading data into the database...")

    if transformed_data is None or transformed_data.empty:
        logging.warning("No data to load. Exiting script.")
        return
    
    data_split = [tuple(row) for row in transformed_data.itertuples(index=False, name=None)]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Insert data into the database, updating existing records on conflict
            query = sql.SQL("""
                            INSERT INTO {} (date, ticker, open, high, low, close, volume, vwap)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (date, ticker) 
                            DO UPDATE SET 
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                volume = EXCLUDED.volume,
                                vwap = EXCLUDED.vwap;
                            """).format(sql.Identifier(table))

            cur.executemany(query, data_split)
            conn.commit()

    logging.info(f'Database updated successfully: {len(transformed_data)} total rows added.')
    logging.info("ETL process completed.")

def run_stocks_etl():
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

if __name__ == '__main__':
    try:
        run_stocks_etl()
    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        sys.exit(1)
