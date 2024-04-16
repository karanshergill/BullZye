import requests
import logging
from sqlalchemy.orm import sessionmaker
from database_Connection import get_engine

# Variables for filtering
exchange_segment = "NSE"
symbol_category = "EQ"

# URL of the JSON file
url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

def fetch_and_filter_data(url, exchange_segment, symbol_category):
    try:
        # Fetching the JSON data
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        return [
            # Filter data based on the variables
            {
                "token": item["token"],
                "symbol": item["symbol"],
                "name": item["name"]
                }
            for item in data if item["exch_seg"] == exchange_segment and symbol_category in item['symbol']
        ]
    except requests.HTTPError as http_err:
        logging.error(f'HTTP error occurred: {http_err}')
    except Exception as err:
        logging.error(f'An error occured: {err}')

def insert_data(engine, filtered_data, market_value="nse", category_value="equity"):
    # SQL query to insert data
    insert_query = """
    INSERT INTO stock_symbols (token, symbol, name, market, category)
    VALUES (:token, :symbol, :name, :market, :category)
    ON CONFLICT (token) DO NOTHING;  -- Assuming 'token' is a unique field
    """
    
    try:
        with sessionmaker(bind=engine)() as session:
            session.bulk_insert_mappings(
                mapper=None,  # SQLAlchemy automatically infers the mapper based on the given objects
                mappings=[
                    {
                        **item,
                        'market': market_value,
                        'category': category_value
                    } for item in filtered_data
                ],
                render_nulls=True,
            )
            session.commit()
            logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the database operation: {e}")

def main():
    engine = get_engine()
    filtered_data = fetch_and_filter_data(url, exchange_segment, symbol_category)
    if filtered_data is not None:
        insert_data(engine, filtered_data)

if __name__ == "__main__":
    main()