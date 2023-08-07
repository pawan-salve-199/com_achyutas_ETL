import os
from dotenv import dotenv_values

# Load environment variables from .env file
path=os.path.dirname(os.path.abspath(__file__))
config = dotenv_values(f"{path}/.env")

# Define a function to get the JDBC configuration
def get_jdbc_config(table_name):
    return {
        "localhost": config["LOCALHOST"],
        "database": config["DATABASE"],
        "table": table_name,
        "user": config["USER"],
        "password": config["PASSWORD"],
    }


