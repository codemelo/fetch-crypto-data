import ccxt
import pandas as pd
from datetime import datetime

# Initialize the Binance exchange
binance = ccxt.binance()

# Get user input for the symbol and timeframe
symbol = input("Enter the trading pair: ")
timeframe = input("Enter the timeframe: ")

# Set the limit for the number of data points per batch
limit = 500  # Number of data points per batch

# Define the start date (adjust as needed)
start_date = '2017-01-01T00:00:00Z'

# Convert start date to milliseconds
since = binance.parse8601(start_date)

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
df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])

# Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Save to CSV
csv_filename = f'binance_{symbol.replace("/", "_")}_{timeframe}_data.csv'
df.to_csv(csv_filename, index=False)

print(f"Data saved to {csv_filename}")
