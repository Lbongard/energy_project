#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  8 20:18:20 2023

@author: Open-Meteo, reidbongard
"""
# Script below is from Open-Meteo's API unless otherwise indicated

import pandas as pd
import numpy as np
import glob
from datetime import datetime, timedelta, timezone, date
from functools import reduce
import requests
from io import StringIO 
import os
import shutil


import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
 	"latitude": 32.80,
 	"longitude": -117.24,
 	"start_date": "2020-04-20",
 	"end_date": "2023-07-31",
 	"hourly": "temperature_2m",
 	"temperature_unit": "fahrenheit"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°E {response.Longitude()}°N")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
 	start = pd.to_datetime(hourly.Time(), unit = "s"),
 	end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
 	freq = pd.Timedelta(seconds = hourly.Interval()),
 	inclusive = "left"
)}
hourly_data["temperature_2m"] = hourly_temperature_2m

hourly_dataframe = pd.DataFrame(data = hourly_data)


# Code below created by author. Sets date as index, adds UTC timezone and exports data

hourly_dataframe.set_index('date', inplace=True)
hourly_dataframe.index = pd.to_datetime(hourly_dataframe.index).tz_localize('UTC')

path = 'Raw_Data/Weather_Data'

if not os.path.exists(path):
    os.makedirs(path)
else:
    shutil.rmtree(path)           # Removes all the subdirectories
    os.makedirs(path)
    
hourly_dataframe.to_csv('Raw_Data/Weather_Data/Weather_Data.csv')




