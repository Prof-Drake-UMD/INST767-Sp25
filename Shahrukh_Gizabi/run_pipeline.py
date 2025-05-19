import os
import time 
import api_client
import data_transformer
import pandas as pd

SPORTSGAMEODDS_API_KEY = os.environ.get("SPORTSGAMEODDS_API_KEY")
FOOTBALL_DATA_ORG_TOKEN = os.environ.get("FOOTBALL_DATA_ORG_TOKEN")
API_FOOTBALL_KEY = os.environ.get("API_FOOTBALL_KEY")

if not SPORTSGAMEODDS_API_KEY:
    print("Warning/Error: SPORTSGAMEODDS_API_KEY environment variable not set.")
if not FOOTBALL_DATA_ORG_TOKEN:
    print("Warning/Error: FOOTBALL_DATA_ORG_TOKEN environment variable not set.")
if not API_FOOTBALL_KEY:
    print("Warning/Error: API_FOOTBALL_KEY environment variable not set.")

OUTPUT_DIR = "output_csvs"

def main():
    print("Starting data ingestion and transformation pipeline...")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

    print("\nProcessing TeamSeasonStats...")
    if not API_FOOTBALL_KEY or "YOUR_API" in API_FOOTBALL_KEY:
        print("API_FOOTBALL_KEY is not set. Skipping TeamSeasonStats.")
    else:
        try:
          
            raw_team_stats_data = api_client.fetch_api_football_team_stats(
                api_key=API_FOOTBALL_KEY,
                league_id="39",
                season="2023",
                team_id="33" 
            )
            if raw_team_stats_data:
                print("Raw data fetched for TeamSeasonStats. Transforming...")
                df_team_stats = data_transformer.transform_team_season_stats_to_df(raw_team_stats_data)
                if not df_team_stats.empty:
                    csv_path = os.path.join(OUTPUT_DIR, "TeamSeasonStats.csv")
                    df_team_stats.to_csv(csv_path, index=False, encoding='utf-8-sig') 
                    print(f"Successfully saved TeamSeasonStats to {csv_path}")
                else:
                    print("No data transformed for TeamSeasonStats. DataFrame is empty.")
            else:
                print("Failed to fetch or no data returned for TeamSeasonStats.")
        except Exception as e:
            print(f"Error processing TeamSeasonStats: {e}")


    print("\nProcessing Matches...")
    if not FOOTBALL_DATA_ORG_TOKEN or "YOUR_FOOTBALL" in FOOTBALL_DATA_ORG_TOKEN:
        print("FOOTBALL_DATA_ORG_TOKEN is not set. Skipping Matches.")
    else:
        try:
            raw_matches_data = api_client.fetch_football_data_matches(
                api_token=FOOTBALL_DATA_ORG_TOKEN,
                competition_code="PL", # Premier League
                season="2023"
            )
            if raw_matches_data:
                print("Raw data fetched for Matches. Transforming...")
                df_matches = data_transformer.transform_matches_to_df(raw_matches_data)
                if not df_matches.empty:
                    csv_path = os.path.join(OUTPUT_DIR, "Matches.csv")
                    df_matches.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f"Successfully saved Matches to {csv_path}")
                else:
                    print("No data transformed for Matches. DataFrame is empty.")
            else:
                print("Failed to fetch or no data returned for Matches.")
        except Exception as e:
            print(f"Error processing Matches: {e}")


    print("\nProcessing MatchEventsAndOdds...")
    if not SPORTSGAMEODDS_API_KEY or "YOUR_SPORTSGAMEODDS" in SPORTSGAMEODDS_API_KEY: 
        print("SPORTSGAMEODDS_API_KEY seems to be a placeholder or not set. Skipping MatchEventsAndOdds.")
   
    try:
        raw_odds_data = api_client.fetch_sports_game_odds(
            api_key=SPORTSGAMEODDS_API_KEY, 
            league_id="EPL", 
            starts_after="2024-08-16", 
            starts_before="2024-08-30" 
        )
        if raw_odds_data:
            print("Raw data fetched for MatchEventsAndOdds. Transforming...")
            df_odds = data_transformer.transform_match_events_odds_to_df(raw_odds_data)
            if not df_odds.empty:
                csv_path = os.path.join(OUTPUT_DIR, "MatchEventsAndOdds.csv")
                df_odds.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"Successfully saved MatchEventsAndOdds to {csv_path}")
            else:
                print("No data transformed for MatchEventsAndOdds. DataFrame is empty.")
        else:
            print("Failed to fetch or no data returned for MatchEventsAndOdds.")
    except Exception as e:
        print(f"Error processing MatchEventsAndOdds: {e}")


    print("\nPipeline finished.")

if __name__ == "__main__":
  
    main()