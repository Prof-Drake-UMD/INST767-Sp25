"""
Test script for API ingestion functions.
This script tests the connection to all three APIs and retrieves sample data.
"""

import os
import sys
from datetime import datetime, timedelta
import json

# Add the src directory to the path so we can import the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import API clients
from Yuting_Shen.src.ingest.sports_api import SportsAPI
from Yuting_Shen.src.ingest.youtube_api import YouTubeAPI
from Yuting_Shen.src.ingest.trends_api import GoogleTrendsAPI


def test_sports_api():
    """Test the Sports API functions."""
    print("\n=== Testing Sports API ===")

    try:
        # Initialize the API client
        api = SportsAPI()

        # Test getting league information (English Premier League)
        print("Getting information for National Football League...")
        league_data = api.get_league("4328")

        if "leagues" in league_data and league_data["leagues"]:
            league = league_data["leagues"][0]
            print(f"✓ League: {league['strLeague']} ({league['strCountry']})")

            # Test getting past events
            print("Getting past events...")
            events_data = api.get_past_events("4328", limit=3)

            if "events" in events_data and events_data["events"]:
                print(f"✓ Retrieved {len(events_data['events'])} events")
                first_event = events_data["events"][0]
                print(f"  First event: {first_event['strEvent']} on {first_event['dateEvent']}")

                # Test getting event details
                event_id = first_event["idEvent"]
                print(f"Getting details for event {event_id}...")
                event_details = api.get_event_details(event_id)

                if "events" in event_details and event_details["events"]:
                    print(f"✓ Retrieved details for event {event_id}")
                else:
                    print(f"✗ Failed to retrieve details for event {event_id}")
            else:
                print("✗ Failed to retrieve events")

            # Test searching for teams
            print("Searching for Manchester United...")
            team_search = api.search_teams("Manchester United")

            if "teams" in team_search and team_search["teams"]:
                print(f"✓ Found {len(team_search['teams'])} matching teams")
                first_team = team_search["teams"][0]
                print(f"  First team: {first_team['strTeam']} ({first_team['strLeague']})")

                # Test getting team details
                team_id = first_team["idTeam"]
                print(f"Getting details for team {team_id}...")
                team_details = api.get_team(team_id)

                if "teams" in team_details and team_details["teams"]:
                    print(f"✓ Retrieved details for team {team_id}")
                else:
                    print(f"✗ Failed to retrieve details for team {team_id}")
            else:
                print("✗ Failed to search for teams")
        else:
            print("✗ Failed to retrieve league information")

        print("Sports API test completed successfully")
        return True

    except Exception as e:
        print(f"✗ Error in Sports API test: {str(e)}")
        return False


def test_youtube_api():
    """Test the YouTube API functions."""
    print("\n=== Testing YouTube API ===")

    try:
        # Check if API key is available
        api_key = os.environ.get('YOUTUBE_API_KEY')
        if not api_key:
            print("✗ YOUTUBE_API_KEY environment variable not set")
            return False

        # Initialize the API client
        api = YouTubeAPI(api_key)

        # Test searching for videos
        print("Searching for Premier League highlights...")
        videos = api.search_sports_videos("Premier League", max_results=3)

        if videos:
            print(f"✓ Retrieved {len(videos)} videos")
            first_video = videos[0]
            video_id = first_video['id']['videoId']
            print(f"  First video: {first_video['snippet']['title']}")

            # Test getting video details
            print(f"Getting details for video {video_id}...")
            details = api.get_video_details(video_id)

            if details:
                print(f"✓ Retrieved details for video {video_id}")
                print(f"  Views: {details['statistics'].get('viewCount', 'unknown')}")
                print(f"  Likes: {details['statistics'].get('likeCount', 'unknown')}")

                # Test getting video comments
                print(f"Getting comments for video {video_id}...")
                comments = api.get_video_comments(video_id, max_results=3)

                if comments:
                    print(f"✓ Retrieved {len(comments)} comments")
                    print(f"  First comment: {comments[0]['text'][:50]}...")
                else:
                    print(f"✗ Failed to retrieve comments")

                # Test getting channel details
                channel_id = details['snippet']['channelId']
                print(f"Getting details for channel {channel_id}...")
                channel = api.get_channel_details(channel_id)

                if channel:
                    print(f"✓ Retrieved details for channel: {channel['snippet']['title']}")

                    # Test getting channel videos
                    print(f"Getting videos from channel {channel_id}...")
                    channel_videos = api.get_channel_videos(channel_id, max_results=3)

                    if channel_videos:
                        print(f"✓ Retrieved {len(channel_videos)} videos from channel")
                    else:
                        print(f"✗ Failed to retrieve videos from channel {channel_id}")
                else:
                    print(f"✗ Failed to retrieve details for channel {channel_id}")
            else:
                print(f"✗ Failed to retrieve details for video {video_id}")
        else:
            print("✗ Failed to search for videos")

        print("YouTube API test completed successfully")
        return True

    except Exception as e:
        print(f"✗ Error in YouTube API test: {str(e)}")
        return False


def test_trends_api():
    """Test the Google Trends API functions."""
    print("\n=== Testing Google Trends API ===")

    try:
        # Initialize the API client
        api = GoogleTrendsAPI()

        # Test getting interest over time
        print("Getting interest over time for 'Kansas City Chiefs'...")
        interest_data = api.get_interest_over_time(["PKansas City Chiefs"], timeframe="today 1-m")

        if not interest_data.empty:
            print(f"✓ Retrieved interest over time data")
            print(f"  Data points: {len(interest_data)}")

            # Test getting interest by region
            print("Getting interest by region...")
            region_data = api.get_interest_by_region(["Kansas City Chiefs"], timeframe="today 1-m")

            if not region_data.empty:
                print(f"✓ Retrieved interest by region data")
                print(f"  Regions: {len(region_data)}")

                # Test getting related queries
                print("Getting related queries...")
                related_queries = api.get_related_queries(["Kansas City Chiefs"], timeframe="today 1-m")

                if related_queries and "Kansas City Chiefs" in related_queries:
                    top_queries = related_queries["Kansas City Chiefs"]["top"]
                    if isinstance(top_queries, pd.DataFrame) and not top_queries.empty:
                        print(f"✓ Retrieved related queries")
                        print(f"  Top queries: {len(top_queries)}")
                    else:
                        print("✗ No top queries found")
                else:
                    print("✗ Failed to retrieve related queries")

                # Test getting trending searches
                print("Getting trending searches for United States...")
                trending_searches = api.get_trending_searches()

                if not trending_searches.empty:
                    print(f"✓ Retrieved trending searches")
                    print(f"  Trending searches: {len(trending_searches)}")
                else:
                    print("✗ Failed to retrieve trending searches")

                # Test getting interest for a team event
                print("Getting interest data for a team event...")
                # Use a date in the past for consistent results
                past_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                event_data = api.get_interest_for_team_event("Kansas City Chiefs", past_date)

                if event_data:
                    print(f"✓ Retrieved team event interest data")
                else:
                    print("✗ Failed to retrieve team event interest data")
            else:
                print("✗ Failed to retrieve interest by region data")
        else:
            print("✗ Failed to retrieve interest over time data")

        print("Google Trends API test completed successfully")
        return True

    except Exception as e:
        print(f"✗ Error in Google Trends API test: {str(e)}")
        return False


def run_all_tests():
    """Run all API tests."""
    print("Starting API tests...\n")

    sports_result = test_sports_api()
    youtube_result = test_youtube_api()
    trends_result = test_trends_api()

    print("\n=== Test Summary ===")
    print(f"Sports API: {'PASSED' if sports_result else 'FAILED'}")
    print(f"YouTube API: {'PASSED' if youtube_result else 'FAILED'}")
    print(f"Google Trends API: {'PASSED' if trends_result else 'FAILED'}")

    all_passed = sports_result and youtube_result and trends_result
    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")

    return all_passed


if __name__ == "__main__":
    # Import pandas here to avoid imports in the test function
    import pandas as pd

    run_all_tests()
