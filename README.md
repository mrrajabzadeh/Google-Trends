# Google Trends Data Downloader

This Python script helps you download and save Google Trends data based on specific keywords and regions. It uses the `pytrends` library to interact with Google Trends API and supports robust error handling to manage different types of errors during data retrieval.

## Features

- Downloads Google Trends data based on specified keywords, dates, locations, and countries.
- Handles multiple keywords and regions through CSV input files.
- Implements error handling and retry logic for a variety of errors, ensuring data is downloaded even if issues occur.
- Supports resuming data download from where it left off by saving progress to a text file.

## Prerequisites

Before running the script, make sure you have the following dependencies installed:

- Python 3.x
- `pandas`: For reading and manipulating CSV files
- `pytrends`: For interacting with Google Trends API
- `requests`: For handling HTTP requests and errors

You can install the required packages using pip:

```bash
pip install pandas pytrends requests
