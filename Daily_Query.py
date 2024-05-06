# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 08:22:05 2024
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

# Set up pytrends
pytrends = TrendReq(hl='en-US', tz=300)

# Set the working directory
new_directory = "C:/Users/mraja/OneDrive/Desktop/Joseph/NEW SVI PROJECT//Daily"
os.chdir(new_directory)

def download_trends_data(keyword, start_date, end_date, Location, Country):
    retries = 0
    max_retries = 5  # Set the maximum number of retries for request attempts

    # Retry loop for robust error handling
    while retries < max_retries:
        try:
            pytrends = TrendReq(hl='en-US', tz=300, timeout=(10, 35))
            kw_list = [keyword]
            overlap = 50
            maxstep = 250
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date() if isinstance(start_date, str) else start_date
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date() if isinstance(end_date, str) else end_date
            step = maxstep - overlap + 1
            new_date = end_date - timedelta(days=step)
            df = pd.DataFrame()

            # Loop to download data for the specified date range in chunks
            while new_date > start_date:
                old_date = new_date + timedelta(days=overlap - 1)
                new_date = new_date - timedelta(days=step)
                timeframe = new_date.strftime('%Y-%m-%d') + ' ' + old_date.strftime('%Y-%m-%d')
                
                # Set output filename prefix for the location data
                out_file_prefix = f"{keyword}_{Location}_all_D_{file_number}"
                pytrends.build_payload(kw_list, cat=0, geo=Location, timeframe=timeframe)
                temp_df = pytrends.interest_over_time()
                if not temp_df.empty:
                    df = pd.concat([temp_df, df], sort=True)

            df.to_csv(out_file_prefix + '.csv')  # Save the collected data to CSV

            # Loop to download data for the entire country in chunks
            while new_date > start_date:
                old_date = new_date + timedelta(days=overlap - 1)
                new_date = new_date - timedelta(days=step)
                timeframe = new_date.strftime('%Y-%m-%d') + ' ' + old_date.strftime('%Y-%m-%d')
                
                # Set output filename prefix for the country data
                out_file_prefix = f"{keyword}_{Country}_all_D_{file_number}"
                pytrends.build_payload(kw_list, cat=0, geo=Country, timeframe=timeframe)
                temp_df = pytrends.interest_over_time()
                if not temp_df.empty:
                    df = pd.concat([temp_df, df], sort=True)

            df.to_csv(out_file_prefix + '.csv')  # Save the collected data to CSV
            
            return df  # Return the collected data as a DataFrame

        # Error handling for various types of exceptions
        except ResponseError as e:
            print(f"The request failed: {e}")
            retries += 1
            if retries < max_retries:
                wait_time = 10  # Exponential backoff
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
            if retries < max-retries:
                wait_time = 5
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            continue

        except RemoteDisconnected as e:
            print(f"RemoteDisconnected error occurred: {e}")
            retries += 1
            if retries < max-retries:
                wait_time = 5
                print(f"Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            continue

    else:  # This else block will be executed if the while loop completes without hitting the break statement
        print("Moving to the next word.")  

# Function to save the index of the last downloaded row
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
for file_number in range(3, 5):
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
