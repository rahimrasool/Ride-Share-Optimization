"""
DATA CLEANING SCRIPT
CS122 PROJECT
TEAM MEMBERS: RAHIM RASOOL, TIRUMALA KAGGUNDI, SILKY AGRAWAL
"""

import os
import sys
import json
import pandas as pd
from google.cloud import bigquery
from sodapy import Socrata
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
    "awesome-shore-306204-28c7b00c79d3.json"


def fetch_raw_data(mode):
    '''
    Uses either google cloud big query or stored csv data to 
      fetch raw data in pandas dataframe

    Input (int): mode takes the argument entered by user
      on what source they want to load data from

    Returns: raw data in pandas dataframe form
    '''
    if mode == "1":
        print("Fetching data using Google cloud, may take 2-4 minutes...")
        client = bigquery.Client()
        query = """
        SELECT trip_start_timestamp, trip_end_timestamp, trip_seconds, \
            trip_miles, pickup_census_tract, dropoff_census_tract, \
            pickup_community_area, dropoff_community_area, fare, trip_total, \
            pickup_latitude, pickup_longitude, dropoff_latitude, \
            dropoff_longitude 
        FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
        WHERE DATE(`trip_start_timestamp`) BETWEEN "2019-09-01" AND \
            "2019-09-21"
        """
        filtered_query = client.query(query)
        iterator = filtered_query.result()
        rows = list(iterator)
        rawdata = pd.DataFrame(data=[list(x.values()) for x in rows], \
            columns=list(rows[0].keys()))
    else:
        print("Fetching data from the repository...")
        rawfile = "raw_data.csv"
        rawdata = pd.read_csv(rawfile)
        rawdata = change_datetime_format(rawdata)

    return rawdata


def clean_data(rawdata):
    '''
    Cleans raw data using various functions below

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): updated raw data to return a
      final cleaned data ready for analysis

    '''
    rawdata = remove_duplicate_rows(rawdata)
    print("1. Removed duplicates, if any")
    data = remove_same_coordinates(rawdata)
    print("2. Removed location with the exact same coordinates, if any")
    data = remove_mv_where_relevant(data)
    print("3. Removed timestamp specific missing data, if any")
    data = remove_inconsistent_time(data)
    print("4. Removed inconsistent start & end time (start time >= end time)")
    data = handle_missing_duration(data)
    print("""5. Imputed missing time with timestamp differences, but removed
        unlikely time differences that were larger than a day or less than 0
        """)
    data = change_tz_to_chicago(data)
    print("6. Changed timestamp from UTC to Chicago time")
    data = decompose_timestamp(data)
    print("7. Decomposed timestamp to date, time, weekend")
    # community_code_mapping = call_comm_area_api()
    # geocode = find_geocode_api_comm_area(community_code_mapping)
    # print("""8. Imputed missing longitude and latitude with community code,
    #     if available (used Google Maps Geocoding API and City of Chicago 
    #     Data API)""")
    # data = impute_mv_lat_long_via_commcode(community_code_mapping, \
    #     geocode, data)
    # census_tract_mapping = call_census_tract_api()
    # print("""9. Imputed missing longitude and latitude with census tract,
    #     if available (using Google Maps Geocoding API and City of Chicago
    #     Data API)""")
    # data = impute_mv_lat_long_via_censustract(community_code_mapping, \
    #     geocode, data, census_tract_mapping)
    print("8. Dropped other missing longitude and latitudes, if missing")
    data = drop_missing_lat_long(data)

    return data


def remove_duplicate_rows(rawdata):
    '''
    Removes any exact duplicate rows present

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): updated raw data after removing
      duplicates
    
    '''
    rawdata = rawdata.drop_duplicates()

    return rawdata


def remove_same_coordinates(rawdata):
    '''
    Removes rows with same pickup and dropoff coordinates

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): updated raw data after removing
      rows with same pickup and dropoff coordinates
    '''
    data = rawdata[(rawdata.pickup_latitude != rawdata.dropoff_latitude) & \
        (rawdata.pickup_longitude != rawdata.dropoff_longitude)]

    return data


def remove_mv_where_relevant(data):
    '''
    Removes any rows where timestamp is not present

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): updated raw data after removing
      rows where timestamp not provided
    '''
    data = data[data.trip_start_timestamp.notnull()]
    data = data[data.trip_end_timestamp.notnull()]

    return data


def remove_inconsistent_time(data):
    '''
    Removes any row where end time is not after the start time
    
    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): updated raw data after removing
      inconsistent timestamps

    '''
    data = data[data.trip_start_timestamp <= data.trip_end_timestamp]

    return data


def handle_missing_duration(rawdata):
    '''
    Imputes trip duration in seconds from timestamp data and removes
      unlikely values

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): updated raw data 

    '''
    end_time = pd.to_datetime(rawdata.trip_end_timestamp)
    start_time = pd.to_datetime(rawdata.trip_start_timestamp)
    fill_value = fill_value = (end_time - start_time).dt.total_seconds()
    rawdata.trip_seconds.fillna(fill_value, inplace=True)
    rawdata = rawdata[(rawdata.trip_seconds < 86400)
                      & (rawdata.trip_seconds > 0)]
    rawdata = rawdata[rawdata.trip_seconds.notnull()]

    return rawdata


def change_tz_to_chicago(data):
    '''
    Changes time zone to Chicago time

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): creates a new column with Chicago time

    '''
    data[
        "trip_start_local_timestamp"] = data.trip_start_timestamp.dt.tz_convert(
            "US/Central")
    data["trip_end_local_timestamp"] = data.trip_end_timestamp.dt.tz_convert(
        "US/Central")

    return data


def decompose_timestamp(data):
    '''
    Decomposes timestamp into date, time and day of the week columns

    Input (pandas dataframe): takes raw data that requires cleaning

    Returns (pandas dataframe): creates new columns for time date and day
      of the week

    '''
    data["start_date"] = data.trip_start_local_timestamp.dt.date
    data["end_date"] = data.trip_end_local_timestamp.dt.date
    data["start_time"] = data.trip_start_local_timestamp.dt.time
    data["end_time"] = data.trip_end_local_timestamp.dt.time
    data["start_weekday"] = data.trip_start_timestamp.dt.dayofweek
    data["end_weekday"] = data.trip_end_timestamp.dt.dayofweek

    return data


def drop_missing_lat_long(data):
    '''
    Drops the finally missing latitude and longitude

    Inputs: pandas updated raw data frame

    Returns: updated final data frame

    '''
    data = data[data.pickup_latitude.notnull()]
    data = data[data.dropoff_latitude.notnull()]

    return data


def change_datetime_format(data):
    '''
    Excel doesn't save columns in datetime format; so when converting csv to
      pandas, this function converts relevant columns to datetime format

    Input: takes pandas data to convert its relevant cols to datetime format 

    Returns: updated raw data in pandas form
    '''
    data["trip_start_timestamp"] = pd.to_datetime(data.trip_start_timestamp)
    data["trip_end_timestamp"] = pd.to_datetime(data.trip_end_timestamp)
    col_list = ["end_time", "start_time", "end_date", "start_date", \
    "trip_start_local_timestamp", "trip_end_local_timestamp", "end_month", "start_month", \
        "end_day", "start_day", "end_hour", "start_hour", "end_minute", "start_minute"]
    for col in col_list:
        if col in data.columns:
            data["end_time"] = pd.to_datetime(data.end_time).dt.time
            data["start_time"] = pd.to_datetime(data.start_time).dt.time
            data["end_date"] = pd.to_datetime(data.end_date).dt.date
            data["start_date"] = pd.to_datetime(data.start_date).dt.date
            data["trip_start_local_timestamp"] = pd.to_datetime(
                data.trip_start_local_timestamp)
            data["trip_end_local_timestamp"] = pd.to_datetime(
                data.trip_end_local_timestamp)
            data["start_weekday"] = data.trip_start_timestamp.dt.dayofweek
            data["end_weekday"] = data.trip_end_timestamp.dt.dayofweek

    return data


def save_cleaned_data(data):
    '''
    Saves the final cleaned data for further analysis

    Inputs (pandas dataframe): final cleaned data

    '''
    filename = "cleaned_data.csv"
    data.to_csv(filename, index=False)


if __name__ == "__main__":
    mode = "2"
    if len(sys.argv) > 1 and sys.argv[1] in ["1", "2"]:
        mode = sys.argv[1]
    rawdata = fetch_raw_data(mode)
    print("Rawdata is received and now cleaning the data...")
    data = clean_data(rawdata)
    save_cleaned_data(data)
    print("Data cleaned and saved for analysis")

