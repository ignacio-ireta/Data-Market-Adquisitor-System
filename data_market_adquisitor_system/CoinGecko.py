import requests
import pandas as pd
from datetime import datetime


def datefromtimestamp(timestamp):
    """Converts from Unix timestamp to MM-DD-YYYY format"""
    date = datetime.utcfromtimestamp(timestamp).strftime('%m-%d-%Y')
    return date


def create(coin_id, against, timeframe):
    # Create a Pandas DataFrame that contains OHCL values for coin_id valued against another currency
    # in a given timeframe
    # Set the API endpoint URL for OHLC
    historical_url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={against}&days={timeframe}'
    # Timeframe can be 1/7/14/30/90/180/365/max
    # Make a get request and save the response
    historical_response = requests.get(historical_url)

    # Verify API call was successful
    if historical_response.status_code == 200:

        # Create DataFrame from JSON response
        historical_df = pd.DataFrame(historical_response.json())
        # Rename DataFrame columns
        historical_df.rename(columns={0: 'Unix_timestamp', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close'}, inplace=True)
        # Convert Unix timestamp to MM-DD-YYYY date
        historical_df['Date'] = (historical_df['Unix_timestamp'] / 1000).apply(datefromtimestamp)
        # Retrieve start and end dates for later volume call
        first = historical_df.Unix_timestamp.iloc[0] / 1000
        last = historical_df.Unix_timestamp.iloc[-1] / 1000
        # Drop Unix timestamp
        historical_df = historical_df.drop(columns=['Unix_timestamp'])

    else:

        return 'Failed to get historical data from the API'

    volume_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range?vs_currency={against}&from={first}&to={last}"
    # Make a get request and save the response
    volume_response = requests.get(volume_url)

    # Verify API call was successful
    if volume_response.status_code == 200:

        # Create DataFrame from JSON response
        volume_df = pd.DataFrame(volume_response.json())
        # Keep only the pertinent information
        volume_df = pd.DataFrame(volume_df['total_volumes'].to_list(), columns=['Unix_timestamp', 'Volume'])
        # Convert Unix timestamp to MM-DD-YYYY date
        volume_df['Date'] = (volume_df['Unix_timestamp'] / 1000).apply(datefromtimestamp)
        # Drop Unix timestamp
        volume_df = volume_df.drop(columns=['Unix_timestamp'])

    else:

        return 'Failed to get volume data from the API'

    return historical_df.join(volume_df.set_index('Date'), on='Date')


whole_df = create('ripple', 'usd', 'max')
whole_df.to_csv(f'coin_OHLCV.csv')
