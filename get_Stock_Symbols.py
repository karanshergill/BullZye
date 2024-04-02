import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from database_Connection import get_engine  # Assuming the first script is saved as DatabaseConnection.py

# Variables for filtering
exchange_segment = "NSE"
symbol_contains = "EQ"

# URL of the JSON file
url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

# Fetching the JSON data
response = requests.get(url)
data = response.json()

# Filter data based on the variables
filtered_data = [
    {"token": item["token"], "symbol": item["symbol"], "name": item["name"]}
    for item in data
    if item["exch_seg"] == exchange_segment and symbol_contains in item["symbol"]
]

# SQL query to insert data
insert_query = """
INSERT INTO stocks (token, symbol, name, market, category)
VALUES (:token, :symbol, :name, :market, :category)
ON CONFLICT (token) DO NOTHING;  -- Assuming 'token' is a unique field
"""

# Market and category values
market_value = "nse"
category_value = "equity"

# Connect to the database using SQLAlchemy
engine = get_engine()
Session = sessionmaker(bind=engine)

try:
    session = Session()
    
    # Prepare data with market and category values
    for item in filtered_data:
        session.execute(
            text(insert_query),
            {
                'token': item['token'],
                'symbol': item['symbol'],
                'name': item['name'],
                'market': market_value,
                'category': category_value
            }
        )
    
    # Commit the transaction
    session.commit()
    print("Data inserted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the session
    if session:
        session.close()
