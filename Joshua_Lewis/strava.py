import os
from dotenv import load_dotenv
from stravalib.client import Client
import pandas as pd
from datetime import datetime

def get_strava_data():
    """
    Fetch activity data from Strava API
    Returns:
        pandas DataFrame: Activities data
    """
    # Load environment variables
    load_dotenv()
    
    # Initialize the Strava client
    client = Client(access_token=os.getenv('STRAVA_ACCESS_TOKEN'))

    try:
        # Get athlete's activities
        activities = client.get_activities()
        
        # Convert activities to DataFrame
        activities_list = []
        for activity in activities:
            activities_list.append({
                'id': activity.id,
                'name': activity.name,
                'start_date': activity.start_date,
                'type': activity.type,
                'distance': float(activity.distance),
                'moving_time': float(activity.moving_time.total_seconds()),
                'elapsed_time': float(activity.elapsed_time.total_seconds()),
                'total_elevation_gain': float(activity.total_elevation_gain),
                'sport_type': activity.sport_type
            })
        
        df = pd.DataFrame(activities_list)
        return df

    except Exception as e:
        print(f"Error fetching Strava data: {e}")
        return None

if __name__ == "__main__":
    activities_df = get_strava_data()
    if activities_df is not None:
        print("\nStrava Activities:")
        print(activities_df.head())