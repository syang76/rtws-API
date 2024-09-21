import requests
import json
import pandas as pd
import sqlite3
from abc import ABC, abstractmethod

# URL of the NOAA's RTWS data in JSON format
url = "https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind-1-hour.json"

# Persistence Layer Abstract Base Class
class PersistenceLayer(ABC):
    @abstractmethod
    def save(self, data, table_name):
        """Save the data to the persistence layer."""
        pass

    @abstractmethod
    def load(self, table_name):
        """Load the data from the persistence layer."""
        pass

# SQLite Persistence Layer
class SQLitePersistenceLayer(PersistenceLayer):
    def __init__(self, db_filename='solar_wind_data.db'):
        self.conn = sqlite3.connect(db_filename)
        self.cursor = self.conn.cursor()

    def save(self, data, table_name):
        data.to_sql(table_name, self.conn, if_exists="replace", index=False)
        print(f"Data saved to {table_name} table in SQLite database.")

    def load(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.conn)
        return df

    def close(self):
        """Close the database connection."""
        self.conn.close()


# Downloads the data from url and converts to list using json()
def fetch_rtws_data(url):
    """Fetch RTWS data from NOAA's SWPC API and return as a Python object."""
    try:
        # Make an HTTP GET request to fetch the raw data
        response = requests.get(url)
        
        # Parse the JSON data
        data = response.json()
        return data
    except:
        print(f"An error occurred: {err}")


# Extracts headers and converts to pandas DataFrame
def process_rtws_data(data):
    """Process the raw RTWS data and convert it into a pandas DataFrame."""
    # First row contains 12 column headers
    headers = data[0]
    # Remaining rows contain the actual data
    records = data[1:]
    # Create a DataFrame for easy manipulation
    df = pd.DataFrame(records, columns=headers)
    return df


# Define App class
class DataApp:
    def __init__(self, persistence_layer):
        self.persistence_layer = persistence_layer

    def run(self):
        # Step 1: Fetch the data
        rtws_data = fetch_rtws_data(url)
        if rtws_data:
            # Step 2: Process the data
            df = process_rtws_data(rtws_data)
            
            # Step 3: Save the data using the persistence layer
            self.persistence_layer.save(df, "solar_wind")
            
            # Step 4: Optionally, load the data back to check it was saved
            loaded_df = self.persistence_layer.load("solar_wind")
            print("Loaded data from the persistence layer:")
            print(loaded_df.head())

    def close(self):
        """Close any open resources."""
        self.persistence_layer.close()

# Main execution
if __name__ == "__main__":
    # Use the SQLite persistence layer for this example
    persistence = SQLitePersistenceLayer("solar_wind_data.db")
    
    app = DataApp(persistence)
    app.run()
    
    app.close()
    
