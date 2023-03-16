from datetime import datetime
import pandas as pd
import requests
import time
    
    
def getohcl(coin_id, vs_currency, days):
    """Returns a Pandas DataFrame with timestamp, open, high, low, close data
    
    Parameters
    
    coin_id : str
        coin_id of the coin in CoinGecko's API
    vs_currency : str
        The target currency of market data (usd, eur, jpy, etc.)
    days : str
        Data up to number of days ago (1/30/max)
    
    Candle's body:
    
    1 day: 30 minutes
    30 days: 4 hours
    max: 4 days
    """
    # Set the API URL for OHLC
    historical_url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency={vs_currency}&days={days}'
    # Make a request and save the response
    historical_response = requests.get(historical_url)
    
    # Verify API call was successful
    if historical_response.status_code == 200:
    
        # Create DataFrame from JSON response
        historical_df = pd.DataFrame(historical_response.json())
        # Rename DataFrame columns
        historical_df.rename(columns={0: 'Unix_timestamp', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close'}, inplace=True)
    
    else:
    
        raise ValueError('Failed to get historical data from the API')
    
    return historical_df
    
    
def getvolume(coin_id, vs_currency, days):
    """Returns a Pandas DataFrame with timestamp, volume data
    
    Parameters
    
    coin_id : str
        coin_id of the coin in CoinGecko's API
    vs_currency : str
        The target currency of market data (usd, eur, jpy, etc.)
    days : str
        Data up to number of days ago (1/30/max)
    
    Candle's body:
    
    1 day from current time = 5 minute interval data
    30 days from current time = hourly data
    max = daily data (00:00 UTC)
    """
    
    volume_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency={vs_currency}&days={days}"
    # Make a get request and save the response
    volume_response = requests.get(volume_url)
    
    # Verify API call was successful
    if volume_response.status_code == 200:
    
        # Create DataFrame from JSON response
        volume_df = pd.DataFrame(volume_response.json())
        # Keep only the pertinent information
        volume_df = pd.DataFrame(volume_df['total_volumes'].to_list(),
                                 columns=['Unix_timestamp', 'Volume'])
    
    else:
    
        raise ValueError('Failed to get volume data from the API')
    
    return volume_df
    
    
def converttimestamp(df):
    df['Date'] = df['Unix_timestamp'].apply(lambda a: int(a/1000)).apply(lambda b: datetime.fromtimestamp(b))
    df = df.drop(columns=['Unix_timestamp'])
    return df
    
    
timeframes = {'30minutes': '1', '4hours': '30', '4days': 'max'}
ticker_vs_currency = ['ripple', 'usd']
for key, value in timeframes.items():
    ohcl = getohcl(*ticker_vs_currency, value)
    ohcl = converttimestamp(ohcl)
    time.sleep(10)
    volume = getvolume(*ticker_vs_currency, value)
    volume = converttimestamp(volume)
    time.sleep(10)
    merged = ohcl.join(volume.set_index('Date'), on='Date')
    merged.to_csv(f'{ticker_vs_currency[0]}_{key}.csv')
