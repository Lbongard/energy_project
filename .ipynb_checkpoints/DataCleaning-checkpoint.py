# -*- coding: utf-8 -*-
"""
Data import, cleaning, and feature engineering for LMP Data

"""
import pandas as pd
import numpy as np
import glob
from datetime import datetime, timedelta, timezone, date
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
    df.reset_index(inplace=True)
    
    return df
    

def utc_to_local(utc_dt):
    """Converts UTC timezone to PST Time"""
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz='America/Los_Angeles')



## First Import RTLMP ##    

rt_df = create_mult_csv_df("./Raw_Data/PACFCBCH_Interval_LMP/")


# Convert starting datetime to datetime type and add new PST version
rt_df['INTERVALSTARTTIME_GMT'] = pd.to_datetime(rt_df['INTERVALSTARTTIME_GMT'] )

# The data download from CAISO contains multiple types of prices. Convert from long to wide format
rt_df = rt_df.pivot(index='INTERVALSTARTTIME_GMT', columns='LMP_TYPE', values='VALUE')

# Add prefix to cols for merging later
rt_df.columns = ["RT_" + col for col in rt_df.columns]

# Create hourly summary to merge wwith DA dataset
hourly_rt_df = rt_df.groupby(rt_df.index.floor("H")).mean()
    
# Checking that LMP is roughly equivalent to the sum of all other lmp components at a given time
sum((rt_df['RT_LMP'] - rt_df[['RT_MCC', 'RT_MCE', 'RT_MCL', 'RT_MGHG']].sum(axis=1)) < 0.001) / rt_df.shape[0]


## Now import DALMP ##

da_df = create_mult_csv_df("./Raw_Data/PACFCBCH_DA_LMP/")

# Convert starting datetime to datetime type and create PST datetime
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

renew_forecast_df = renew_forecast_df[renew_forecast_df['LABEL'].isin(['Renewable Forecast Day Ahead', 'Renewable Forecast Actual Generation'])]

# Pivoting on hub, renewable type and market

renew_forecast_df = renew_forecast_df.pivot(index='INTERVALSTARTTIME_GMT', columns=["TRADING_HUB", "RENEWABLE_TYPE", "LABEL"], values="MW")

renew_forecast_df.columns = ["_".join(col) for col in renew_forecast_df.columns]


## Importing Weather Data ##

year_list = [2020, 2021, 2022, 2023]

weather_df_list = []

for year in year_list:

    
    dates_query = lambda year: f'&startDate={year}-01-01T00:00:00Z&endDate={year}-12-31T11:59:00Z'

    # Check if hourly normals is right
    base_url = "https://www.ncei.noaa.gov/access/services/data/v1/?dataset=normals-hourly"
    data_types = "&datatypes=HLY-CLDH-NORMAL, HLY-HTDH-NORMAL, HLY-TEMP-NORMAL"
    # Stations include LAX airport and San Diego Int'l Airport
    stations="&stations=USW00023174,USW00023188"
    dates=dates_query(year)
    bounding_box="&boundingBox=90,-180,-90,180"
    units="&units=standard"
    
    query_str = base_url+data_types+stations+dates+bounding_box+units
    
    r = requests.get(query_str)
    weather_df = pd.read_csv(StringIO(r.text))
    
    # Adjusting date formating to match other dataframes
    weather_df['DATE'] = weather_df['DATE'].apply(lambda x: datetime.strptime(x,"%m-%dT%X"))
    weather_df['DATE'] = weather_df['DATE'].apply(lambda x: x.replace(year=year))

    
    weather_df_list.append(weather_df)

    
# Combining all above weather queries into one dataframe
weather_df = pd.concat(weather_df_list)

# Non-zero numbers that round to 0 are marked as -777.7
weather_df_describe = pd.DataFrame(weather_df.describe())
weather_df_describe.loc['min']

# Replace -777.7 values with 0
weather_df.replace(-777.7, 0, inplace=True)
weather_df_describe = pd.DataFrame(weather_df.describe())
weather_df_describe.loc['min']


# Keep temperature, cooling degree hours, and heating degree hours for each station
keep_cols = ['DATE', 'HLY-CLDH-NORMAL', 'HLY-HTDH-NORMAL', 'HLY-TEMP-NORMAL', 'STATION']
weather_df.drop(axis=1, columns=weather_df.columns[~weather_df.columns.isin(keep_cols)], inplace=True)
weather_df = weather_df.pivot(index='DATE', columns='STATION', values=['HLY-CLDH-NORMAL', 'HLY-HTDH-NORMAL', 'HLY-TEMP-NORMAL'])

weather_df.columns = ["_".join(col) for col in weather_df.columns]

# Add timezone (UTC for now)
weather_df.index = weather_df.index.tz_localize('UTC')


# Create Hourly DataFrame

df_list = [hourly_rt_df, da_df, hourly_load_df, renew_forecast_df, weather_df]
df = reduce(lambda left, right: pd.merge(left, right, 
                                           left_index=True, 
                                           right_index=True), df_list)


# Convert time to local
df.index = df.index.to_series().apply(utc_to_local)



#### Feature Engineering ####

# LMP Spike Indicators
df['RTLMP_spike_50_binary'] = df['RT_LMP'].apply(lambda x: 1 if x >= 50 else 0)
df['RTLMP_spike_75_binary'] = df['RT_LMP'].apply(lambda x: 1 if x >= 75 else 0)
df['RTLMP_spike_100_binary'] = df['RT_LMP'].apply(lambda x: 1 if x >= 100 else 0)
df['RTLMP_spike_150_binary'] = df['RT_LMP'].apply(lambda x: 1 if x >= 150 else 0)

# Create a variable for weekends
df['friday'] = np.where(df.index.weekday==4, 1, 0)
df['weekend']  = np.where(df.index.weekday>4, 1, 0)
df['hour'] = df.index.hour


# Create a variable for hour types
df['on_peak_hour'] = np.where(((df.hour>=16) & (df.hour<=21)), 1, 0)

# Create a variable for month
df['month'] = df.index.month

# Sum wind and solar generation across zones to get "Total" that is more indicative of overall CAISO generation
df['Total_Solar_Actual'] = df['NP15_Solar_Renewable Forecast Actual Generation'] + df['SP15_Solar_Renewable Forecast Actual Generation'] + df['ZP26_Solar_Renewable Forecast Actual Generation']
df['Total_Solar_Forecast'] = df['NP15_Solar_Renewable Forecast Day Ahead'] + df['SP15_Solar_Renewable Forecast Day Ahead'] + df['ZP26_Solar_Renewable Forecast Day Ahead']

df['Total_Wind_Actual'] = df['NP15_Wind_Renewable Forecast Actual Generation'] + df['SP15_Wind_Renewable Forecast Actual Generation']
df['Total_Wind_Forecast'] = df['NP15_Wind_Renewable Forecast Day Ahead'] + df['SP15_Wind_Renewable Forecast Day Ahead']

# Calculate sum of wind and solar actuals / forecasts
df['Total_Wind_Solar_Actual'] = df['Total_Solar_Actual'] + df['Total_Wind_Actual']
df['Total_Wind_Solar_Forecast'] = df['Total_Solar_Forecast'] + df['Total_Wind_Forecast']

# Calculate Wind / Solar Forecast Error
df['renew_forecast_error'] = df['Total_Wind_Solar_Actual'] - df['Total_Wind_Solar_Forecast']
df['solar_forecast_error'] = df['Total_Solar_Actual'] - df['Total_Solar_Forecast']
df['wind_forecast_error'] = df['Total_Wind_Actual'] - df['Total_Wind_Forecast']



# Write function to create lagged and future variables

def create_shifted_series(df, col_name, shift_hours):
    """Create a column of col_name that is shifted 'shift_hours' in the future"""
    shifted_series = pd.Series(index=df.index)

    for idx, row in df.iterrows():
        shifted_time = idx + pd.Timedelta(hours=shift_hours)
        
        if shifted_time in df.index:
            shifted_value = df.loc[shifted_time, col_name]
        else:
            shifted_value = None
        
        shifted_series[idx] = shifted_value

    return shifted_series


## Use function to create lagged variables for forecast error, import, export and generation

lag_var_list = ['RT_LMP', 'DA_LMP', 'renew_forecast_error', 'Export', 'Generation', 'Import']

for var in lag_var_list:
    for lag_hrs in [-2, -4, -12, -20, -22, -23]:
        col_name = "lagged_" + str(-lag_hrs)+"hr_"+var
        df[col_name] = create_shifted_series(df=df, col_name=var, shift_hours=lag_hrs)
        

# Use function to create lagged variables for weather variables

weather_vars = ['HLY-CLDH-NORMAL_USW00023174','HLY-CLDH-NORMAL_USW00023188',
                'HLY-HTDH-NORMAL_USW00023174', 'HLY-HTDH-NORMAL_USW00023188',
                'HLY-TEMP-NORMAL_USW00023174','HLY-TEMP-NORMAL_USW00023188']

for var in weather_vars:
    for lag_hrs in [-2, -4, -12, -22, -23]:
        col_name = "lagged_" + str(-lag_hrs)+"hr_"+var
        df[col_name] = create_shifted_series(df=df, col_name=var, shift_hours=lag_hrs)
        

# Create encoded (cyclical) features for month and hour variables        
df['sin_month'] = np.sin(2 * np.pi * df['month']/12.0)
df['cos_month'] = np.cos(2 * np.pi * df['month']/12.0)
df['sin_hour'] = np.sin(2 * np.pi * df['hour']/24.0)
df['cos_hour'] = np.cos(2 * np.pi * df['hour']/24.0)

# Create future variables as target variables
df['DA_LMP_in_12_hrs'] = create_shifted_series(df=df, col_name='DA_LMP', shift_hours=12)
df['DA_LMP_in_2_hrs'] = create_shifted_series(df=df, col_name='DA_LMP', shift_hours=2)

df['RT_LMP_in_12_hrs'] = create_shifted_series(df=df, col_name='RT_LMP', shift_hours=12)
df['RT_LMP_in_2_hrs'] = create_shifted_series(df=df, col_name='RT_LMP', shift_hours=2)

# Target Hour
df['target_hour'] = create_shifted_series(df=df, col_name='hour', shift_hours=2)

# Target Friday and Weekend Indicators
df['target_friday'] = create_shifted_series(df=df, col_name='friday', shift_hours=2)
df['target_weekend'] = create_shifted_series(df=df, col_name='weekend', shift_hours=2)

# Target Month
df['target_month'] = create_shifted_series(df=df, col_name='month', shift_hours=2)

# Create encoded (cyclical) features for month and hour variables
df['target_sin_month'] = create_shifted_series(df=df, col_name='sin_month', shift_hours=2)
df['target_cos_month'] = create_shifted_series(df=df, col_name='cos_month', shift_hours=2)
df['target_sin_hour'] = create_shifted_series(df=df, col_name='sin_hour', shift_hours=2)
df['target_cos_hour'] = create_shifted_series(df=df, col_name='cos_hour', shift_hours=2)
        
# Format data timezone index and column names
df.index = pd.to_datetime(df.index, format='%Y-%m-%d', utc=True)
df.index = df.index.tz_convert("America/Los_Angeles")
df.columns = ['_'.join(colname.split()) for colname in df.columns]
df.columns = [colname.replace('-','_') for colname in df.columns]


df.to_csv('./Cleaned_Data/LMP_and_feature_data.csv')
