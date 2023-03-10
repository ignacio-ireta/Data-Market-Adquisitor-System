import requests
import pandas as pd
from datetime import datetime


def datefromtimestamp(timestamp):
    """Converts from Unix timestamp to MM-DD-YYYY format"""
    date = datetime.utcfromtimestamp(timestamp).strftime('%m-%d-%Y')
    return date


# Set the URL parameters for the desired coin/exchange pair/timeframe
coin_id = 'ripple'
against = 'usd'
timeframe = 'max'


# Set the API endpoint URL and parameters
url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={against}&days={timeframe}'


# Make the API call and get the response
response = requests.get(url)


# Verify API call was successful
if response.status_code == 200:
    coin_df = pd.DataFrame(response.json())
else:
    print('Failed to get data from the API')

# Rename DataFrame columns
coin_df.rename(columns={0: 'Unix_timestamp', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close'}, inplace=True)
# Convert Unix timestamp to MM-DD-YYYY date
coin_df['Date'] = (coin_df['Unix_timestamp']/1000).apply(datefromtimestamp)
# Drop Unix timestamp column
coin_df.drop(columns='Unix_timestamp')
