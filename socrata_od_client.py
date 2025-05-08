"""socrata_od_client.py

Simple wrapper for the MTA Subway Origin-Destination ridership dataset.
Returns data as a pandas DataFrame.
"""
import sys
sys.dont_write_bytecode = True

import requests
import pandas as pd
import io

# List of columns to query from the API
QUERY_COLUMNS = [
    "year",
    "month",
    "day_of_week",
    "hour_of_day",
    "origin_station_complex_id",
    "origin_station_complex_name",
    "destination_station_complex_id",
    "destination_station_complex_name",
    "estimated_average_ridership"
]

def get_ridership_data(
    year: int = None,
    month: int = None,
    day_of_week: str = None,
    hour_of_day: int = None,
    origin_station_complex_id: int = None,
    destination_station_complex_id: int = None,
    app_token: str = None
) -> pd.DataFrame:
    """
    Fetch MTA ridership data from the Socrata API.
    
    Parameters
    ----------
    year : int, optional
        Filter by year
    month : int, optional
        Filter by month (1-12)
    day_of_week : str, optional
        Filter by day type ('Weekday', 'Saturday', 'Sunday')
    hour_of_day : int, optional
        Filter by hour_of_day (0-23)
    origin_station_complex_id : int, optional
        Filter by origin station complex ID
    destination_station_complex_id : int, optional
        Filter by destination station complex ID
    app_token : str, optional
        Socrata application token for higher rate limits
        
    Returns
    -------
    pandas.DataFrame
        DataFrame containing the ridership data
    """
    # Base URL for the dataset
    url = "https://data.ny.gov/resource/y2qv-fytt.csv"
    
    # Build query parameters
    params = {}
    if app_token:
        params['$$app_token'] = app_token
        
    # Add column selection
    params['$select'] = ','.join(QUERY_COLUMNS)
        
    # Build where clause
    where_parts = []
    if year is not None:
        where_parts.append(f"year = {year}")
    if month is not None:
        where_parts.append(f"month = {month}")
    if day_of_week is not None:
        where_parts.append(f"day_of_week = '{day_of_week}'")
    if hour_of_day is not None:
        where_parts.append(f"hour_of_day = {hour_of_day}")
    if origin_station_complex_id is not None:
        where_parts.append(f"origin_station_complex_id = {origin_station_complex_id}")
    if destination_station_complex_id is not None:
        where_parts.append(f"destination_station_complex_id = {destination_station_complex_id}")
        
    if where_parts:
        params['$where'] = ' AND '.join(where_parts)
    
    # Make the request
    response = requests.get(url, params=params)
    response.raise_for_status()
    
    # Read CSV into DataFrame
    df = pd.read_csv(io.StringIO(response.text))
    
    # Convert numeric columns
    numeric_cols = ['year', 'month', 'hour_of_day', 'origin_station_complex_id', 
                   'destination_station_complex_id', 'estimated_trips']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

# Example usage
if __name__ == "__main__":
    # Get weekday ridership for May 2024 at 9am from station 623
    df = get_ridership_data(
        year=2024,
        month=5,
        hour_of_day=9,
        day_of_week="Weekday",
        origin_station_complex_id=623
    )
    print(df.head())
