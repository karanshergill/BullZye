import pandas as pd
from database_Connection import get_engine
from sqlalchemy import text
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of public holidays for 2022, 2023, and 2024
public_holidays = [
    '2022-01-26', '2022-03-01', '2022-03-18', '2022-04-14', '2022-04-15', '2022-05-03',
    '2022-08-09', '2022-08-15', '2022-08-31', '2022-10-05', '2022-10-24', '2022-10-26',
    '2022-11-08',
    '2023-01-26', '2023-03-07', '2023-03-30', '2023-04-04', '2023-04-07', '2023-04-14',
    '2023-05-01', '2023-06-29', '2023-08-15', '2023-09-19', '2023-10-02', '2023-10-24',
    '2023-11-14', '2023-11-27', '2023-12-25',
    '2024-01-22', '2024-01-26', '2024-03-08', '2024-03-25', '2024-03-29', '2024-04-11',
    '2024-04-17', '2024-05-01', '2024-06-17', '2024-07-17', '2024-08-15', '2024-10-02',
    '2024-11-01', '2024-11-15', '2024-12-25'
]

# Generate a date range from 2022 to 2024, excluding weekends and public holidays
date_range = pd.date_range(start='2022-01-01', end='2024-12-31', freq='B')  # 'B' frequency excludes weekends
filtered_dates = [date for date in date_range if date.strftime('%Y-%m-%d') not in public_holidays]

# Connect to the database
engine = get_engine()

# Insert the filtered dates into the table
insert_query = """
INSERT INTO market_trading_calendar (trading_dates)
VALUES (:trading_dates)
ON CONFLICT (trading_dates) DO NOTHING;
"""

# Function to insert dates with batch processing and error recovery
def insert_dates_with_recovery(dates):
    batch_size = 100  # Process 100 entries at a time
    for start_index in range(0, len(dates), batch_size):
        try:
            with engine.begin() as connection:
                connection.execute(
                    text(insert_query),
                    [{'trading_dates': date} for date in dates[start_index:start_index + batch_size]]
                )
            logging.info(f"Inserted dates from index {start_index} to {start_index + batch_size}")
        except Exception as e:
            logging.error(f"An error occurred at batch starting index {start_index}: {e}")
            # Optional: Decide if you want to retry the failed batch or skip to the next one
            continue  # Continue with the next batch if failure occurs

# Insert dates with error recovery
insert_dates_with_recovery(filtered_dates)
