import ccxt
import pandas as pd
from datetime import datetime

# Initialize the Binance exchange
binance = ccxt.binance()

# Specify the symbol and timeframe
symbol = 'ETH/USDT'
timeframe = '1h'
limit = 500  # Number of data points per batch

# Define the start and end dates (adjust as needed)
start_date = '2017-01-01T00:00:00Z'
#end_date = '2024-06-10T00:00:00Z'

# Convert dates to milliseconds
since = binance.parse8601(start_date)
#end_timestamp = binance.parse8601(end_date)

# Container for all fetched data
all_ohlcv = []

batch_count = 0

while True:
    # Fetch historical data
    ohlcv = binance.fetch_ohlcv(symbol, timeframe, since, limit)
    
    # Break if no more data is returned
    if not ohlcv:
        print("No more data fetched. Exiting loop.")
        break
    
    # Append the fetched data to the container
    all_ohlcv.extend(ohlcv)
    
    # Update 'since' to the last timestamp fetched + 1ms to avoid overlap
    since = ohlcv[-1][0] + 1
    
    batch_count += 1
    print(f"Batch {batch_count}: Fetched {len(ohlcv)} rows. Last timestamp: {pd.to_datetime(ohlcv[-1][0], unit='ms')}")

# Convert to DataFrame
df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Ensure that 'end_date' is also a timezone-naive datetime
#end_date_naive = pd.to_datetime(end_date).tz_localize(None)

# Filter data to ensure it does not go beyond the end date
#df = df[df['timestamp'] >= end_date_naive]

# Save to CSV
df.to_csv('binance_ETH_1h_data.csv', index=False)

print("Data saved to binance_ETH_1h_data.csv")
