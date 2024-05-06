# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 17:37:15 2024

@author: mraja
"""

import pandas as pd
from pytrends.request import TrendReq
import os
from datetime import datetime, timedelta
import sys
from pytrends.exceptions import TooManyRequestsError
from requests.exceptions import ProxyError, Timeout
import ssl
from requests.exceptions import ProxyError, Timeout
import time
import http.client
from pytrends.exceptions import ResponseError
from requests.exceptions import ChunkedEncodingError, RequestException, Timeout

# Set up pytrends
pytrends = TrendReq(hl='en-US', tz=300)

# Set the working directory to the desired location
new_directory = "C:/Users/mraja/OneDrive/Desktop/Joseph/NEW SVI PROJECT//Monthly"
os.chdir(new_directory)

import time
import requests
from requests.exceptions import ProxyError, Timeout

def download_trends_data(keyword, start_date, end_date, Location, Country):
    # Retry logic setup
    retries = 0
    max_retries = 5  # Max number of retries for each request

    while retries < max_retries:
        try:
            # Download data for a specific location
            out_file_prefix = f"{keyword}_{Location}_all_M_{file_number}"
            pytrends = TrendReq(hl='en-US', tz=300, timeout=(10, 35))
            kw_list = [keyword]
            timeframe = start_date + ' ' + end_date
            pytrends.build_payload(kw_list, cat=0, geo=Location, timeframe=timeframe)
            interest_over_time_df = pytrends.interest_over_time()
            if not interest_over_time_df.empty:
                interest_over_time_df.to_csv(out_file_prefix + '.csv')
             
            # Download data for the entire country
            out_file_prefix = f"{keyword}_{Country}_all_M_{file_number}"
            pytrends.build_payload(kw_list, cat=0, geo=Country, timeframe=timeframe)
            interest_over_time_df = pytrends.interest_over_time()
            if not interest_over_time_df.empty:
                interest_over_time_df.to_csv(out_file_prefix + '.csv')
            related_queries_dict = pytrends.related_queries()
            top_queries_df = related_queries_dict[keyword]['top']
            if top_queries_df is not None:
                top_queries_df.to_csv(out_file_prefix + '_top_queries.csv')
         
            # Download data for the location using different category
            out_file_prefix = f"{keyword}_{Location}_Inv_M_{file_number}"
            pytrends = TrendReq(hl='en-US', tz=300, timeout=(10, 35))
            kw_list = [keyword]
            timeframe = start_date + ' ' + end_date
            pytrends.build_payload(kw_list, cat=107, geo=Location, timeframe=timeframe)
            interest_over_time_df = pytrends.interest_over_time()
            if not interest_over_time_df.empty:
                interest_over_time_df.to_csv(out_file_prefix + '.csv')
             
            # Download data for the entire country using different category
            out_file_prefix = f"{keyword}_{Country}_Inv_M_{file_number}"
            pytrends.build_payload(kw_list, cat=107, geo=Country, timeframe=timeframe)
            interest_over_time_df = pytrends.interest_over_time()
            if not interest_over_time_df.empty:
                interest_over_time_df.to_csv(out_file_prefix + '.csv')
            break  # Successfully downloaded, exit the loop

        # Handle specific exceptions and retry logic with exponential backoff
        except ResponseError as e:
            print(f"The request failed: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 10
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            continue
        except ChunkedEncodingError as e:
            print(f"Chunked Encoding Error: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 10
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
        except Timeout as e:
            print(f"Timeout Error: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 10
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
        except RequestException as e:
            print(f"Request Error: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 10
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
        except (ProxyError, Timeout) as e:
            print(f"Error occurred for keyword {keyword}: {e}")
            retries += 1
            wait_time = 5
            print(f"Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
        except ssl.SSLEOFError as e:
            print(f"SSL/TLS connection closed unexpectedly for keyword {keyword}: {e}")
            retries += 1
            wait_time = 5
            print(f"Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            continue
        except TooManyRequestsError:
            print(f"Too many requests for keyword {keyword}. Waiting for 15 seconds...")
            retries += 1
            time.sleep(15)
            continue

        except ValueError as e:
            print(f"A value error occurred for keyword {keyword}: {e}")
            retries += 1
            continue
        except http.client.RemoteDisconnected as e:
            print(f"Remote end closed connection for keyword {keyword}: {e}")
            retries += 1
            wait_time = 5
            print(f"Waiting for {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            continue  
        except requests.exceptions.ConnectionError as e:
            print(f"ConnectionError occurred: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 5
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            continue
        except ReadTimeout as e:
            print(f"ReadTimeout occurred: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 5
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            continue
        except RemoteDisconnected as e:
            print(f"RemoteDisconnected error occurred: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 5
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            continue

    else:  # All retries failed
        print("Moving to the next word.")  

def save_last_downloaded_index(file_number, index):
    """Save the index of the last downloaded row for a specific file number."""
    with open(f'last_downloaded_{file_number}.txt', 'w') as f:
        f.write(str(index))

def get_last_downloaded_index(file_number):
    """Get the index of the last downloaded row for a specific file number."""
    try:
        with open(f'last_downloaded_{file_number}.txt', 'r') as f:
            return int(f.read())
    except FileNotFoundError:
        return -1  # Return -1 if the file does not exist, indicating a fresh start.

# Loop through each file and download data for each keyword
for file_number in range(1, 5):
    filename = f'tickerlist{file_number}.csv'
    df = pd.read_csv(filename, header=None)

    # Reset or read the last downloaded index for resuming
    last_downloaded_index = get_last_downloaded_index(file_number)  
    if last_downloaded_index == 2500:
        continue

    # Iterate over each row in the DataFrame and download data
    for index, row in df.iterrows():
        if index <= last_downloaded_index:
            continue

        keyword = row[0]  # Assuming keyword is in the first column
        start_year = 2019  # Fixed start year
        start_month = row[2]  # Start month
        start_day = row[3]  # Start day
        end_year = 2024  # Fixed end year
        end_month = row[5]  # End month
        end_day = row[6]  # End day
        Location = row[7]  # US state
        Country = row[8]  # Country

        start_date = f"{int(start_year)}-{int(start_month):02d}-{int(start_day):02d}"
        end_date = f"{int(end_year)}-{int(end_month):02d}-{int(end_day):02d}"

        download_trends_data(keyword, start_date, end_date, Location, Country)
        save_last_downloaded_index(file_number, index)
