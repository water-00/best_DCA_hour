import requests
import time
import pandas as pd
from datetime import datetime, timedelta

# Bitget API URL for history candlestick data
CRYPTO = "DOGE"
API_URL = "https://api.bitget.com/api/spot/v1/market/history-candles"
DAYS = 360
CSV_FILE = f"{CRYPTO}_360_DAYS.csv"

# Parameters for the request
symbol = f"{CRYPTO}USDT_SPBL"
period = "1h"            # 1-hour granularity
limit = "120"            # 每次取120条, 5天的1h数据

# Requesting the data in chunks (max limit of 200 items per request)
def getData():
    # Set end_time as 2024-12-10 00:00:00 (UTC+8)
    end_time_utc8 = datetime(2024, 12, 10, 0, 0, 0)  # 2024-12-10 0:00:00 UTC+8
    end_time_utc = end_time_utc8 - timedelta(hours=8)  # Convert to UTC
    end_time = int(end_time_utc.timestamp() * 1000)    # Convert to timestamp in milliseconds
    start_time = int((end_time_utc - timedelta(days=DAYS)).timestamp() * 1000)

    # List to store the results
    candlestick_data = []
    
    iteration = 0
    while start_time < end_time:
        iteration += 1
        # Prepare the request parameters
        params = {
            'symbol': symbol,
            'period': period,
            'endTime': end_time,
            'limit': limit
        }
        
        # Make the API request
        response = requests.get(API_URL, params=params)
        
        if response.status_code == 200:
            data = response.json().get('data', [])
            
            if not data:
                print("No more data available.")
                break
            
            # Add data to the list
            candlestick_data.extend(data)
            
            # Update the endTime for the next batch
            end_time = int(data[0]['ts'])
            
            # Print progress information
            latest_time = pd.to_datetime(data[-1]['ts'], unit='ms') + timedelta(hours=8)  # Convert to UTC+8
            print(f"Iteration {iteration}: Fetched {len(data)} records. Latest timestamp: {latest_time}")
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            break
        
        # Sleep to avoid hitting the rate limit
        time.sleep(0.01)

    open_prices = [item['open'] for item in candlestick_data]
    df = pd.DataFrame(candlestick_data)
    df['datetime'] = pd.to_datetime(df['ts'].astype(int), unit='ms') + timedelta(hours=8)

    # Extract just the columns we need: datetime and open price
    df_open = df[['datetime', 'open']].sort_values(by='datetime', ascending=True)
    df_open.to_csv(CSV_FILE, index=False)

    print(df_open)  # Print the first few rows of data
    
def calBestHour(days=360):
    # 读取数据
    df = pd.read_csv(CSV_FILE)
    
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    # 获取研究范围的起始时间
    end_date = df['datetime'].max()  # 数据的最后时间
    start_date = end_date - timedelta(days=days)  # 起始时间为最近的 days 天

    df = df[df['datetime'] >= start_date]
    df['hour'] = df['datetime'].dt.hour

    # 初始化结果存储
    breakeven_prices = {}

    # 遍历每个小时 (0-23)
    for hour in range(24):
        hour_data = df[df['hour'] == hour]['open']

        # breakeven = days * 24 / (sum(1/i for i in prices))
        total_investment = len(hour_data)  # 定投次数 (days 天 * 每天24小时)
        harmonic_sum = sum(1 / price for price in hour_data)
        breakeven_price = total_investment / harmonic_sum

        breakeven_prices[hour] = breakeven_price

    breakeven_df = pd.DataFrame(list(breakeven_prices.items()), columns=['Hour', 'Breakeven Price'])
    # print(breakeven_df)
    
    # 找到最高和最低的价格及对应小时
    max_price = breakeven_df['Breakeven Price'].max()
    max_hour = breakeven_df.loc[breakeven_df['Breakeven Price'].idxmax(), 'Hour']
    min_price = breakeven_df['Breakeven Price'].min()
    min_hour = breakeven_df.loc[breakeven_df['Breakeven Price'].idxmin(), 'Hour']

    # 计算百分比差距
    percentage_difference = ((max_price - min_price) / min_price) * 100

    # 打印结果
    print(f"Days: {days}")
    print(f"Highest Breakeven Price: {max_price:.5f} at hour {max_hour}")
    print(f"Lowest Breakeven Price: {min_price:.5f} at hour {min_hour}")
    print(f"Percentage Difference: {percentage_difference:.2f}%")
    
if __name__ == "__main__":
    getData()
    calBestHour(days=360)
    calBestHour(days=180)
    calBestHour(days=90)
    calBestHour(days=45)