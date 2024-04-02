import os
from dotenv import load_dotenv

load_dotenv()

# SmartApi Constants
api_key=os.getenv('API_KEY')
username=os.getenv('USERNAME')
pwd=os.getenv('PASSWORD')
authentication_token=os.getenv('TOKEN')

# Database Constants
db_host=os.getenv('DB_HOST')
db_name=os.getenv('DB_NAME')
db_user=os.getenv('DB_USER')
db_password=os.getenv('DB_PASSWORD')
db_port=os.getenv('DB_PORT')
