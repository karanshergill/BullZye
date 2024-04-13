# Database Structure
Create the tables as below:

1. Stock Symbols
```sql
CREATE TABLE stock_symbols (
    id SERIAL PRIMARY KEY,
    token INT UNIQUE NOT NULL,
    name VARCHAR(255),
    symbol VARCHAR(25),
    exchange VARCHAR(3),
    category VARCHAR(10)
);
```

2. Historical Data - One Minute Interval
```sql
CREATE TABLE stocks_historical_data_one_minute_interval (
    timestamp TIMESTAMPTZ NOT NULL,
    token INT NOT NULL,
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume BIGINT,
    PRIMARY KEY (timestamp, token)
);
```

3. Historical Data - One Day Interval
```sql
CREATE TABLE stocks_historical_data_one_day_interval (
    date DATE NOT NULL,
    token INT NOT NULL,
    open_price NUMERIC(10, 2),
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    close_price NUMERIC(10, 2),
    volume BIGINT,
    PRIMARY KEY (date, token)
);
```