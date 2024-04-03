import logging
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from environment_Configuration import db_host, db_name, db_user, db_password, db_port

# Function to get database parameters from `EnvironmentConfiguration.py`
def get_database_parameters():
    """Function to get database parameters."""
    return {
        'database': db_name,
        'username': db_user,
        'password': db_password,
        'host': db_host,
        'port': db_port
    }

def test_database_connection(engine):
    try:
        # Attempt to connect to the database and execute a simple query
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            logging.info("Database connection test successful:", result.scalar() == 1)
    except Exception as e:
        logging.error("Database connection test failed:", e)

def get_engine():
    try:
        db_params = get_database_parameters()
        connection_url = URL.create(drivername="postgresql", **db_params)
        engine = create_engine(connection_url, pool_size=10, max_overflow=20)
        return engine
    except Exception as e:
        logging.error("Failed to create engine: %s", e)
        raise

if __name__ == "__main__":
    try:
        engine = get_engine()
        test_database_connection(engine)
    except Exception as e:
        logging.error("Unexpected error: %s", e)