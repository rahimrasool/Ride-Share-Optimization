import json
import pandas as pd
from datetime import datetime

def fetch_data():
    rawdata = pd.read_csv("chicago_taxi_raw.csv")
    rawdata = rawdata.drop_duplicates()
    data = rawdata[(rawdata.pickup_latitude != rawdata.dropoff_latitude) & \
        (rawdata.pickup_longitude != rawdata.dropoff_longitude)]
    return rawdata

def format_datetime(data):
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
            data["trip_start_local_timestamp"] = pd.to_datetime(data.trip_start_local_timestamp)
            data["trip_end_local_timestamp"] = pd.to_datetime(data.trip_end_local_timestamp)
            data["start_weekday"] = data.trip_start_timestamp.dt.dayofweek
            data["end_weekday"] = data.trip_end_timestamp.dt.dayofweek

    data = data[data.trip_start_timestamp.notnull()]
    data = data[data.trip_end_timestamp.notnull()]
    data = data[data.trip_start_timestamp <= data.trip_end_timestamp]

    end_time = pd.to_datetime(data.trip_end_timestamp)
    start_time = pd.to_datetime(data.trip_start_timestamp)
    fill_value = fill_value = (end_time - start_time).dt.total_seconds()
    data.trip_seconds.fillna(fill_value, inplace=True)
    data = data[(data.trip_seconds < 86400) & (data.trip_seconds > 0)]
    data = data[data.trip_seconds.notnull()]

    data["trip_start_local_timestamp"] = data.trip_start_timestamp.dt.tz_convert("US/Central")
    data["trip_end_local_timestamp"] = data.trip_end_timestamp.dt.tz_convert("US/Central")

    data["start_date"] = data.trip_start_local_timestamp.dt.date
    data["end_date"] = data.trip_end_local_timestamp.dt.date
    data["start_time"] = data.trip_start_local_timestamp.dt.time
    data["end_time"] = data.trip_end_local_timestamp.dt.time
    data["start_weekday"] = data.trip_start_timestamp.dt.dayofweek
    data["end_weekday"] = data.trip_end_timestamp.dt.dayofweek

    return data

def clean_location(data):
    data = data[data.pickup_latitude.notnull()]
    data = data[data.dropoff_latitude.notnull()]
    return data

def perform_cleaning():
    taxi_data_1 = fetch_data()
    taxi_data_2 = format_datetime(taxi_data_1)
    taxi_data_3 = clean_location(taxi_data_2)
    taxi_data_3.to_csv("cleaned_data.csv", index = False)

if __name__ == "__main__":
    perform_cleaning()
    print("Data Cleaning Complete ..........")