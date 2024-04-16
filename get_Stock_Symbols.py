import requests
import logging
from sqlalchemy import text
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
        logging.error(f'An error occurred: {err}')

def insert_data(session, filtered_data, exchange="nse", category="equity"):
    try:
        count = 0
        for item in filtered_data:
            existing_symbol = session.execute(
                text("SELECT COUNT(*) FROM stock_symbols WHERE token = :token"),
                {'token': item['token']}
            ).scalar()
            if existing_symbol == 0:
                session.execute(
                    text("""
                    INSERT INTO stock_symbols (token, symbol, name, exchange, category)
                    VALUES (:token, :symbol, :name, :exchange, :category);
                    """),
                    {
                        'token': item['token'],
                        'symbol': item['symbol'],
                        'name': item['name'],
                        'exchange': exchange,
                        'category': category
                    }
                )
                count += 1
        session.commit()
        logging.info(f"{count} data inserted successfully.")
        return count
    except Exception as e:
        session.rollback()
        logging.error(f"An error occurred during the database operation: {e}")
        return 0

def main():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    with Session() as session:
        filtered_data = fetch_and_filter_data(url, exchange_segment, symbol_category)
        if filtered_data:
            count = insert_data(session, filtered_data)
            print(f"Total {count} stock symbols inserted.")

if __name__ == "__main__":
    main()
