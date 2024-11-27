import os
from pymongo import MongoClient
from dotenv import load_dotenv

def connect_to_databases():
    """
    Connects to the geo-distributed MongoDB databases and returns a dictionary of MongoDB client objects.
    
    Returns:
        dict: A dictionary containing MongoDB client objects for the database clusters.
    
    Raises:
        Exception: If a connection to any database fails.
    """
    load_dotenv()

    connections = {
        "east": os.getenv("EAST_DB_URI"),
        "central": os.getenv("CENTRAL_DB_URI"),
        "west": os.getenv("WEST_DB_URI"),
        "main": os.getenv("MAIN_DB_URI") 
    }

    db_clients = {}

    for name, uri in connections.items():
        if not uri:
            raise Exception(f"Connection string for {name} is missing in the environment variables.")
        try:
            client = MongoClient(uri)
            # Test the connection
            client.admin.command('ping')
            db_clients[name] = client
        except Exception as e:
            raise Exception(f"Failed to connect to {name} database: {e}")
    
    print("Successfully connected to all databases.")
    return db_clients


if __name__ == "__main__":
    databases = connect_to_databases()