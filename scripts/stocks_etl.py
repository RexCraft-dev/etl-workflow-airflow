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
    # Establish a connection to the PostgreSQL database.
    try:
        return connect(database=db, user=user, password=passwd)
    except Exception as e:
        logging.error(f"Database connection failed: {e}", exc_info=True)
        sys.exit(1)

def extract():
    # Extract stock data from Yahoo Finance based on the last available date in the database.
    logging.info("ETL process started.")
    logging.info(f"Processing tickers: {tickers}")
    
    raw_data = None

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
                continue

            # Add ticker column
            df['ticker'] = ticker

            if raw_data is None:
                raw_data = df
            else:
                raw_data = pd.concat([raw_data, df], ignore_index=True)

            logging.info(f'[{ticker}] Data downloaded successfully. ({len(df)})')

        except Error as db_err:
            logging.error(f"Database query failed for [{ticker}]: {db_err}")
        except Exception as e:
            logging.error(f"Unexpected error during extraction for [{ticker}]: {e}", exc_info=True)

    if raw_data is None:
        logging.info("All tickers returned empty data. Exiting script.")
        logging.info("ETL process completed.")
        sys.exit(0)

    return raw_data

def transform(raw_data):
    # Transform raw stock data by renaming columns and calculating VWAP.
    logging.info("Transforming data...")

    raw_data['vwap'] = round((raw_data['Close'] * raw_data['Volume']).cumsum() / raw_data['Volume'].cumsum(), 2)

    # Rename columns to match database schema
    raw_data.rename(columns={'Date': 'date', 
                        'Close': 'close', 
                        'High': 'high', 
                        'Low': 'low', 
                        'Open': 'open', 
                        'Volume': 'volume'}, 
                        inplace=True)
    
    cols = ['date', 'ticker', 'open', 'high', 
            'low', 'close', 'volume', 'vwap']
    transformed_data = raw_data[cols]

    return transformed_data

def load(transformed_data):
    # Load transformed stock data into the PostgreSQL database.
    logging.info("Loading data into the database...")

    if transformed_data is None or transformed_data.empty:
        logging.warning("No data to load. Exiting script.")
        return
    
    data_split = [tuple(row) for row in transformed_data.itertuples(index=False, name=None)]

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Insert data into the database, updating existing records on conflict
                query = sql.SQL("""
                                INSERT INTO {} (date, ticker, open, high, low, close, volume, vwap)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (date, ticker) DO NOTHING;
                                """).format(sql.Identifier(table))

                cur.executemany(query, data_split)
                conn.commit()

                logging.info(f'Database updated successfully: {len(transformed_data)} total rows added.')
                
    except Error as db_err:
        logging.error(f"Database Error: {db_err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)

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
