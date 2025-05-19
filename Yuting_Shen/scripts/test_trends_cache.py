"""
Test the cached Google Trends API.
"""

import logging
import os
import sys
import time


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("trends_cache_test.log")
    ]
)

# Import the API
from Yuting_Shen.src.ingest.trends_api import GoogleTrendsAPI


def test_cached_api():
    """Test the cached API with multiple requests."""
    print("Testing Google Trends API with caching...")

    # Initialize API with a short cache interval for testing
    api = GoogleTrendsAPI(
        cache_dir="data/trends_cache",
        cache_max_age_days=7,  # Cache data is valid for 7 days
        request_interval=30  # Only 10 seconds between requests for testing
    )

    # Test with a single keyword
    print("\n1. First request (should fetch from API):")
    data1 = api.get_interest_over_time("NFL")
    if not data1.empty:
        print(f"✓ Retrieved data with {len(data1)} rows")
    else:
        print("✗ Failed to retrieve data")

    # Test the same request again (should use cache)
    print("\n2. Second request with same parameters (should use cache):")
    data2 = api.get_interest_over_time("NFL")
    if not data2.empty:
        print(f"✓ Retrieved data with {len(data2)} rows")
    else:
        print("✗ Failed to retrieve data")

    # Test with different parameters
    # print("\n3. Request with different parameters (should fetch from API):")
    # data3 = api.get_interest_over_time(["NBA", "NFL"], timeframe="today 3-m")
    # if not data3.empty:
    #     print(f"✓ Retrieved data with {len(data3)} rows and {len(data3.columns) - 1} keywords")
    # else:
    #     print("✗ Failed to retrieve data")

    # Test interest by region
    print("\n4. Testing interest by region:")
    region_data = api.get_interest_by_region("NFL")
    if not region_data.empty:
        print(f"✓ Retrieved region data with {len(region_data)} regions")
    else:
        print("✗ Failed to retrieve region data")

    # # Test related queries
    # print("\n5. Testing related queries:")
    # query_data = api.get_related_queries("NBA Finals")
    # if query_data and "NBA Finals" in query_data:
    #     print("✓ Retrieved related queries data")
    # else:
    #     print("✗ Failed to retrieve related queries")

    # Test throttling by making requests in quick succession
    # print("\n6. Testing request throttling (should wait between requests):")
    # for i in range(3):
    #     print(f"Request {i + 1}...")
    #     start_time = time.time()
    #     data = api.get_interest_over_time(f"test query {i}")
    #     end_time = time.time()
    #     print(f"Request completed in {end_time - start_time:.2f} seconds")

    print("\nCache test completed")


if __name__ == "__main__":
    test_cached_api()