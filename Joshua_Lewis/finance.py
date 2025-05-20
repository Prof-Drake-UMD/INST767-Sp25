import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta

def get_market_data(api_key, date=None):
    """
    Fetch daily market aggregates from Polygon.io
    Args:
        api_key (str): Polygon.io API key
        date (str): Date in format YYYY-MM-DD, defaults to yesterday
    Returns:
        pandas DataFrame: Daily market data
    """
    client = RESTClient(api_key)

    # Use yesterday's date if none provided
    if not date:
        yesterday = datetime.now() - timedelta(days=1)
        date = yesterday.strftime('%Y-%m-%d')

    try:
        aggs = client.get_grouped_daily_aggs(
            locale='us',
            market_type='stocks',
            date=date
        )

        market_data = []
        for agg in aggs.results:
            market_data.append({
                'ticker': agg['T'],
                'close_price': agg['c'],
                'open_price': agg['o'],
                'highest_price': agg['h'],
                'lowest_price': agg['l'],
                'trading_volume': agg['v'],
                'vwap': agg.get('vw', None),
                'transactions': agg.get('n', None),
                'date': date
            })

        df = pd.DataFrame(market_data)
        return df

    except Exception as e:
        print(f"Error fetching market data: {e}")
        return None

# Example usage
if __name__ == "__main__":
    API_KEY = "CrlPqKaRdXp2G1H2sTUGUneJxficKSLd"  # Replace with your own key
    market_df = get_market_data(API_KEY)
    
    if market_df is not None:
        print("\nMarket Data Summary:")
        print(f"Total stocks analyzed: {len(market_df)}")
        print("\nSample of market data:")
        print(market_df.head())

        # Stats
        print("\nMarket Statistics:")
        print(f"Average closing price: ${market_df['close_price'].mean():.2f}")
        print(f"Total trading volume: {market_df['trading_volume'].sum():,}")
