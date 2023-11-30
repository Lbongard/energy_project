# -*- coding: utf-8 -*-
"""
Data import, cleaning, and feature engineering for LMP Data

"""
import pandas as pd
import numpy as np
import glob
from datetime import datetime, timedelta
from functools import reduce
import requests

from io import StringIO 

##### Import all LMP files and combine into a single DataFrame #####

# Create function for concatenating multiple csv's into one dataframe

def create_mult_csv_df(filepath):
    """Loops through folder specified in filepath and returns a df that concatenates all csv's in that file"""
    
    # Create list to store df's from each individual df
    temp_df_list = []
    
    # Create path that searches for all csv's in folder
    search_path = filepath + "*csv" 
    
    # Add df's from individual csv's to list
    for file in glob.glob(search_path):
        temp_df = pd.read_csv(file)
        temp_df_list.append(temp_df)
        
    df = pd.concat(temp_df_list) # Combine all df's
    
    return df
    


## First Import RTLMP ##    

rt_df = create_mult_csv_df("./Raw_Data/SP15_interval_LMP/")

rt_df_08_07 = rt_df[rt_df['OPR_DT'] == '2022-08-07']
np.unique(rt_df_08_07['OPR_HR'])


# Convert starting datetime to datetime type
rt_df['INTERVALSTARTTIME_GMT'] = pd.to_datetime(rt_df['INTERVALSTARTTIME_GMT'] )

# The data download from CAISO contains multiple types of prices. Convert from long to wide format
rt_df = rt_df.pivot(index='INTERVALSTARTTIME_GMT', columns='LMP_TYPE', values='VALUE')

# Add prefix to cols for merging later
rt_df.columns = ["RT_" + col for col in rt_df.columns]

# Checking that LMP is roughly equivalent to the sum of all other lmp components at a given time
sum((rt_df['RT_LMP'] - rt_df[['RT_MCC', 'RT_MCE', 'RT_MCL', 'RT_MGHG']].sum(axis=1)) < 0.001) / rt_df.shape[0]

# Create hourly summary to merge wwith DA dataset
hourly_rt_df = rt_df.groupby(rt_df.index.floor("H")).mean()

## Now import DALMP ##

da_df = create_mult_csv_df("./Raw_Data/SP15_DA_LMP/")

# Convert starting datetime to datetime type
da_df['INTERVALSTARTTIME_GMT'] = pd.to_datetime(da_df['INTERVALSTARTTIME_GMT'])

da_df = da_df.pivot(index = 'INTERVALSTARTTIME_GMT', columns='LMP_TYPE', values='MW')

# Add prefix to cols for merging later
da_df.columns = ["DA_" + col for col in da_df.columns]



## CAISO Load ##

load_df = create_mult_csv_df("./Raw_Data/CAISO_LOAD/")

load_df['INTERVALSTARTTIME_GMT'] = pd.to_datetime(load_df['INTERVALSTARTTIME_GMT'])

# FIltering for only total CAISO load for now
load_df = load_df[load_df['TAC_ZONE_NAME'] == 'Caiso_Totals']

# Getting wide data with import/gen/export as columns
load_df = load_df.pivot(index='INTERVALSTARTTIME_GMT', columns='SCHEDULE', values='MW')

# Create hourly summary to merge wwith DA dataset
hourly_load_df = load_df.groupby(load_df.index.floor("H")).mean()

## Wind and Solar Forecast ##

renew_forecast_df = create_mult_csv_df("./Raw_Data/Wind_Solar_Forecast/")

renew_forecast_df['INTERVALSTARTTIME_GMT'] = pd.to_datetime(renew_forecast_df['INTERVALSTARTTIME_GMT'])

renew_forecast_df = renew_forecast_df[renew_forecast_df['LABEL'] == 'Renewable Forecast RTD']

# Pivoting on hub, renewable type and market

renew_forecast_df = renew_forecast_df.pivot(index='INTERVALSTARTTIME_GMT', columns=["TRADING_HUB", "RENEWABLE_TYPE", "LABEL"], values="MW")

renew_forecast_df.columns = ["_".join(col) for col in renew_forecast_df.columns]

## Wind and Solar Dispatch ##
renew_df = create_mult_csv_df('./Raw_Data/Wind_Solar_Dispatch/')
renew_df = renew_df[renew_df['MARKET_RUN_ID'] == 'RTD'] # Filter for only RTD market
renew_df.rename({"VALUE":"renewables_rtd"}, axis=1, inplace=True)
renew_df['INTERVALSTARTTIME_GMT'] = pd.to_datetime(renew_df['INTERVALSTARTTIME_GMT'])
renew_df.set_index('INTERVALSTARTTIME_GMT', inplace=True)
renew_df = renew_df['renewables_rtd']

## Importing Weather Data ##

# Check if hourly normals is right
base_url = "https://www.ncei.noaa.gov/access/services/data/v1/?dataset=normals-hourly"
data_types = "&datatypes=HLY-CLDH-NORMAL, HLY-HTDH-NORMAL, HLY-TEMP-NORMAL"
# Stations include LAX airport, San Diego Int'l Airport, and Long Beach Airport
stations="&stations=USW00023174,USW00023188,USW00023129"
dates="&startDate=2022-08-01&endDate=2022-09-04"
bounding_box="&boundingBox=90,-180,-90,180"
units="&units=standard"

query_str = base_url+data_types+stations+dates+bounding_box+units

r = requests.get(query_str)
weather_df = pd.read_csv(StringIO(r.text))
# Adjusting date formating to match other dataframes
weather_df['DATE'] = weather_df['DATE'].apply(lambda x: datetime.strptime(x,"%m-%dT%X"))
weather_df['DATE'] = weather_df['DATE'].apply(lambda x: x.replace(year=2022))
# weather_df.set_index('DATE', inplace=True)


# Keep temperature, cooling degree hours, and heating degree hours for each station
keep_cols = ['DATE', 'HLY-CLDH-NORMAL', 'HLY-HTDH-NORMAL', 'HLY-TEMP-NORMAL', 'STATION']
weather_df.drop(axis=1, columns=weather_df.columns[~weather_df.columns.isin(keep_cols)], inplace=True)
weather_df = weather_df.pivot(index='DATE', columns='STATION', values=['HLY-CLDH-NORMAL', 'HLY-HTDH-NORMAL', 'HLY-TEMP-NORMAL'])

weather_df.columns = ["_".join(col) for col in weather_df.columns]
weather_df.index = pd.to_datetime(weather_df.index).tz_localize('GMT')

# Join real-time and day-ahead data


df_list = [hourly_rt_df, da_df, hourly_load_df, renew_forecast_df, renew_df, weather_df]
df = reduce(lambda left, right: pd.merge(left, right, 
                                           left_index=True, 
                                           right_index=True), df_list)


#### Feature Engineering ####
# DART Spread, Lagged variables, time of day,

df['DART'] = df['DA_LMP'] - df['RT_LMP']


# Create a variable for weekends
df['friday'] = np.where(df.index.weekday==4, 1, 0)
df['weekend']  = np.where(df.index.weekday>4, 1, 0)

# Create variable for time of day
df['hour'] = df.index.hour

# Forecast Error
df['Total_Solar_Forecast_RTD'] = df['NP15_Solar_Renewable Forecast RTD'] + df['SP15_Solar_Renewable Forecast RTD'] + df['ZP26_Solar_Renewable Forecast RTD']
df['Total_Wind_Forecast_RTD'] = df['NP15_Wind_Renewable Forecast RTD'] + df['SP15_Wind_Renewable Forecast RTD']
df['Total_Wind_Solar_Forecast_RTD'] = df['Total_Solar_Forecast_RTD'] + df['Total_Wind_Forecast_RTD']

df['renew_forecast_error'] = df['renewables_rtd'] - df['Total_Wind_Solar_Forecast_RTD']

# Write function to create lagged variables

def create_day_lagged_var(df, lag_var, lag_days=1):
    """ Creates a lagged variable by the specified number of days"""
    
    curr_var = df.copy()[[lag_var]] # First extract current value of variable
    curr_var['next_day'] = curr_var.index + timedelta(days=lag_days) # Get timestamp specified number of days in advance
    
    # Create new Raw_DataFrame indexed on the next day
    prev_day_var = curr_var.set_index('next_day')
    
    # Specify that variable is previous day's value in new DataFrame
    new_var_name = "prev_day_" + prev_day_var.columns.values[0]
    prev_day_var.rename(columns={prev_day_var.columns.values[0]:new_var_name},  inplace=True)
    
    return prev_day_var


# Use function to create lagged variables for forecast error, import, export and generation
prev_day_forecast_error = create_day_lagged_var(df=df, lag_var='renew_forecast_error', lag_days=1)
prev_day_load = create_day_lagged_var(df=df, lag_var='renew_forecast_error', lag_days=1)

lag_var_list = ['renew_forecast_error', 'Export', 'Generation', 'Import']

for var in lag_var_list:
    lag_df = create_day_lagged_var(df=df, lag_var=var, lag_days=1)
    df = pd.merge(df, lag_df, left_index=True, right_index=True, how='left')


# df.to_csv('./Cleaned_Data/dart_and_feature_data.csv')
