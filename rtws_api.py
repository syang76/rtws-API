#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 21 01:36:23 2024

@author: syang
"""

import requests
import json
import pandas as pd

# URL of the NOAA's RTWS data
url = "https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind-1-hour.json"


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


# Saves the processed data into a CSV file
def save_data_to_csv(df, filename="solar_wind_data.csv"):
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


def main():
    # Fetch the data
    rtws_data = fetch_rtws_data(url)
    
    if rtws_data:
        # Process the data into a DataFrame
        df = process_rtws_data(rtws_data)
        
        # Display the first few rows of the DataFrame
        print("First few rows of the RTWS data:")
        print(df.head())

        # Optionally, save the data to a CSV file
        save_data_to_csv(df)

if __name__ == "__main__":
    main()
    
