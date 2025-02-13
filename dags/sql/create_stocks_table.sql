-- Create stocks table
CREATE TABLE IF NOT EXISTS stocks (
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    vwap NUMERIC,
    PRIMARY KEY (date, ticker)
);
