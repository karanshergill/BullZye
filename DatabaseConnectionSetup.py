from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from EnvironmentConfiguration import db_host, db_name, db_user, db_password, db_port

# Function to get database parameters from `EnvironmentConfiguration.py`
def Get_Databse_Parameters():
    return {
        'database': db_name,
        'username': db_user,
        'password': db_password,
        'host': db_host,
        'port': db_port
    }

def Test_Database_Connection(engine):
    try:
        # Attempt to connect to the database and execute a simple query
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Database connection test successful:", result.scalar() == 1)
    except Exception as e:
        print("Database connection test failed:", e)

try:
    db_params = Get_Databse_Parameters()
    connection_url = URL.create(drivername="postgresql", **db_params)
    engine = create_engine(connection_url, pool_size=10, max_overflow=20)
    Session = sessionmaker(bind=engine)
    
    Test_Database_Connection(engine)
except EnvironmentError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")

def Get_Engine():
    return engine