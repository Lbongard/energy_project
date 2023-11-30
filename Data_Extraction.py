#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 18:28:34 2022

@author: reidbongard
"""
import calendar
import os
import shutil
import requests
from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen
import time
import pandas as pd

# Get Date Ranges for each month in dataset
# CAISO API allows for 15 day limits. Therefore, generate pairs in 15 day increments

def get_date_pairs(start_date, end_date):
    """
    Generates date pairs in 15-day increments between start_date and end_date (inclusive).
    
    Parameters:
        start_date (str): Start date in the format 'YYYY-MM-DD'.
        end_date (str): End date in the format 'YYYY-MM-DD'.
        
    Returns:
        list of tuples: List of date pairs in the format (start_date, end_date).
    """
    date_pairs = []
    current_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    while current_date <= end_date:
        next_date = current_date + pd.DateOffset(days=14)
        if next_date > end_date:
            next_date = end_date

        date_pairs.append((current_date.strftime('%Y%m%d'), next_date.strftime('%Y%m%d')))
        current_date = next_date + pd.DateOffset(days=1)

    return date_pairs

# Generate pairs of dates. Apr 20, 2020 is first day of available data
date_pairs = get_date_pairs(start_date='2020-04-20', end_date='2023-07-31')


## Real-Time Locational Marginal Price ##
rtlmp_query = lambda start, end: f'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_INTVL_LMP&version=3&startdatetime={start}T08:00-0000&enddatetime={end}T08:00-0000&market_run_id=RTM&node=PACFCBCH_6_N004'

path = 'Raw_Data/PACFCBCH_Interval_LMP'
if not os.path.exists(path):
    os.makedirs(path)
else:
    shutil.rmtree(path)           # Removes all the subdirectories
    os.makedirs(path)


for date_pair in date_pairs:
    start = date_pair[0]
    end = date_pair[1]
    
    api_url = rtlmp_query(start, end)

    # Allow up to 3 tries (included so that script can be stopped / started from command line)
    try_count = 3

    while try_count > 0:
        try:
            r = requests.get(api_url, stream=True)
            try_count = 0
            
        except ChunkedEncodingError as ex:
            if try_count <=0:
                print("Failed to retrieve: " + api_url + "\n" + str(ex))  # done retrying
            else:
                try_count -=1
            time.sleep(0.5)

    z = ZipFile(BytesIO(r.content))
    z.extractall('./Raw_Data/PACFCBCH_Interval_LMP')

    # Sleep to control crawl rate
    time.sleep(3)
    
## Day-Ahead Locational Marginal Price ##
dalmp_query = lambda start, end: f'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime={start}T08:00-0000&enddatetime={end}T08:00-0000&market_run_id=DAM&node=PACFCBCH_6_N004'

path = 'Raw_Data/PACFCBCH_DA_LMP'
if not os.path.exists(path):
    os.makedirs(path)
else:
    shutil.rmtree(path)           # Removes all the subdirectories
    os.makedirs(path)


for date_pair in date_pairs:
    start = date_pair[0]
    end = date_pair[1]
    
    api_url = dalmp_query(start, end)
    
    # Allow up to 3 tries (included so that script can be stopped / started from command line)
    try_count = 3

    while try_count > 0:
        try:
            r = requests.get(api_url, stream=True)
            try_count = 0
        except ChunkedEncodingError as ex:
            if try_count <=0:
                print("Failed to retrieve: " + api_url + "\n" + str(ex))  # done retrying
            else:
                try_count -=1
            time.sleep(0.5)

    z = ZipFile(BytesIO(r.content))
    z.extractall('./Raw_Data/PACFCBCH_DA_LMP')
    
    # Sleep to control crawl rate
    time.sleep(3)
    

## CAISO Load ##

load_query = lambda start, end: f'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ENE_SLRS&version=1&market_run_id=RTM&tac_zone_name=ALL&schedule=Export,Generation,Import,Load&startdatetime={start}T08:00-0000&enddatetime={end}T08:00-0000'

path = 'Raw_Data/CAISO_LOAD'
if not os.path.exists(path):
    os.makedirs(path)
else:
    shutil.rmtree(path)           # Removes all the subdirectories
    os.makedirs(path)


for date_pair in date_pairs:
    start = date_pair[0]
    end = date_pair[1]
    
    api_url = load_query(start, end)
    
    # Allow up to 3 tries (included so that script can be stopped / started from command line)
    try_count = 3

    while try_count > 0:
        try:
            r = requests.get(api_url, stream=True)
            try_count = 0
        except ChunkedEncodingError as ex:
            if try_count <=0:
                print("Failed to retrieve: " + api_url + "\n" + str(ex))  # done retrying
            else:
                try_count -=1
            time.sleep(0.5)

    z = ZipFile(BytesIO(r.content))
    z.extractall('./Raw_Data/CAISO_LOAD_v2')

    # Sleep to control crawl rate
    time.sleep(3)
    
## Wind and Solar Forecast ##

renew_fcst_query = lambda start, end: f'http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_REN_FCST&version=1&startdatetime={start}T08:00-0000&enddatetime={end}T08:00-0000'

path = 'Raw_Data/Wind_Solar_Forecast_v2'
if not os.path.exists(path):
    os.makedirs(path)
else:
    shutil.rmtree(path)           # Removes all the subdirectories
    os.makedirs(path)


for date_pair in date_pairs:
    start = date_pair[0]
    end = date_pair[1]
    
    api_url = renew_fcst_query(start, end)
    
    # Allow up to 3 tries (included so that script can be stopped / started from command line)
    try_count = 3

    while try_count > 0:
        try:
            r = requests.get(api_url, stream=True)
            try_count = 0
        except ChunkedEncodingError as ex:
            if try_count <=0:
                print("Failed to retrieve: " + api_url + "\n" + str(ex))  # done retrying
            else:
                try_count -=1
            time.sleep(0.5)

    z = ZipFile(BytesIO(r.content))
    z.extractall('./Raw_Data/Wind_Solar_Forecast')

    # Sleep to control crawl rate
    time.sleep(3)







