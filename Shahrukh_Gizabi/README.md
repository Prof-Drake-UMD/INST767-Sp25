# Soccer Data Integration Project

This project integrates data from three soccer APIs to analyze team performance statistics, primarily focusing on the Premier League. The pipeline fetches, transforms, and loads data into BigQuery for analysis, aiming to identify what stats represent a winning team.

## Data Sources

Below are the API data sources integrated in this project:

### 1. API-Football (by API-Sports)

* **Purpose:** Detailed team statistics (form, fixtures, goals, lineups, cards).
* **Key Inputs Used:**
    * API Key (Header: `x-rapidapi-key`)
    * League ID (e.g., "39" for Premier League)
    * Season Year (e.g., "2023")
    * Team ID (e.g., "33")
* **Expected Key Data Output (Example JSON Snippet):**
    ```json
    {
      "league": {
        "id": 39,
        "name": "Premier League",
        "country": "England",
        "season": 2023
      },
      "team": {
        "id": 33,
        "name": "Manchester United"
      },
      "form": "WLWLLWLWWL...",
      "fixtures": {
        "played": {"home": 19, "away": 19, "total": 38},
        "wins": {"home": 10, "away": 8, "total": 18},
        "draws": {"home": 3, "away": 3, "total": 6},
        "loses": {"home": 6, "away": 8, "total": 14}
      },
      "goals": {
        "for": {
          "total": {"home": 31, "away": 26, "total": 57},
          "average": {"home": "1.6", "away": "1.4", "total": "1.5"},
          "minute": {
            "0-15": {"total": 7, "percentage": "12.07%"},
            "76-90": {"total": 14, "percentage": "24.14%"}
          }
        },
        "against": {
          "total": {"home": 28, "away": 30, "total": 58},
          "average": {"home": "1.5", "away": "1.6", "total": "1.5"}
        }
      },
      "clean_sheet": {"home": 4, "away": 5, "total": 9},
      "lineups": [
        {"formation": "4-2-3-1", "played": 32}
      ]
    }
    ```
* **Documentation:** [API-Football Docs](https://www.api-football.com/documentation-v3#tag/Teams/operation/get-teams-statistics)

### 2. Football-Data.org API

* **Purpose:** Match results, schedules, scores, and competition details.
* **Key Inputs Used:**
    * API Token (Header: `X-Auth-Token`)
    * Competition Code (e.g., "PL" for Premier League)
    * Season Year (e.g., "2023")
* **Expected Key Data Output (Example JSON Snippet for one match):**
    ```json
    {
      "area": {
        "id": 2072,
        "name": "England",
        "code": "ENG"
      },
      "competition": {
        "id": 2021,
        "name": "Premier League",
        "code": "PL"
      },
      "season": {
        "id": 1564,
        "startDate": "2023-08-11",
        "endDate": "2024-05-19",
        "currentMatchday": 38
      },
      "id": 435943,
      "utcDate": "2023-08-11T19:00:00Z",
      "status": "FINISHED",
      "matchday": 1,
      "homeTeam": {
        "id": 328,
        "name": "Burnley FC",
        "shortName": "Burnley"
      },
      "awayTeam": {
        "id": 65,
        "name": "Manchester City FC",
        "shortName": "Man City"
      },
      "score": {
        "winner": "AWAY_TEAM",
        "fullTime": {"home": 0, "away": 3},
        "halfTime": {"home": 0, "away": 2}
      },
      "referees": [
        {"id": 11585, "name": "Craig Pawson", "type": "REFEREE"}
      ]
    }
    ```
* **Documentation:** [Football-Data.org Docs](https://www.football-data.org/documentation/quickstart)

### 3. Sports Game Odds API (sportsgameodds.com)

* **Purpose:** Betting odds for matches, including various markets and player props.
* **Key Inputs Used:**
    * API Key (Query Param: `apiKey`)
    * League ID (e.g., "EPL")
    * Date Range (`startsAfter`, `startsBefore`)
* **Expected Key Data Output (Example JSON Snippet for one event, focusing on odds):**
    ```json
    {
      "eventID": "n7D1xG8G6DP4PkFsI2rg",
      "sportID": "SOCCER",
      "leagueID": "EPL",
      "info": {
        "seasonWeek": "Premier League 24/25"
      },
      "teams": {
        "home": {"names": {"long": "Manchester United"}, "teamID": "MANCHESTER_UNITED_EPL"},
        "away": {"names": {"long": "Fulham"}, "teamID": "FULHAM_EPL"}
      },
      "odds": {
        "points-home-game-ml-home": {
          "marketName": "Moneyline (Full Match)",
          "statEntityID": "home",
          "periodID": "game",
          "bookOdds": "-5000", 
          "byBookmaker": {
            "fanduel": {"odds": "-2400", "lastUpdatedAt": "2024-08-16T20:48:41.000Z"},
            "draftkings": {"odds": "-20000", "lastUpdatedAt": "2024-08-16T20:50:17.000Z"}
          }
        },
        "points-all-game-ou-under": {
          "marketName": "Over/Under (Full Match)",
          "statEntityID": "all",
          "periodID": "game",
          "bookOdds": "-1107",
          "bookOverUnder": "1.5",
          "byBookmaker": {
            "pinnacle": {"odds": "-450", "overUnder": "1.5", "lastUpdatedAt": "2024-08-16T20:51:55.000Z"}
          }
        },
        "shots_onGoal-BRUNO_FERNANDES_1_EPL-game-ou-over": {
          "marketName": "Bruno Fernandes Shots On Goal Over/Under (Full Match)",
          "playerID": "BRUNO_FERNANDES_1_EPL",
          "bookOdds": "-211",
          "bookOverUnder": "0.5"
        }
      }
    }
    ```

* **Documentation:** [Sports Game Odds API Docs](https://sportsgameodds.apidocumentation.com/reference)

---