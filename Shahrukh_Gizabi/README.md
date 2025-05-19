# Soccer Data Integration Project

This project aims to integrate data from three different soccer APIs to create a combined data model for analysis, with the ultimate goal of identifying what stats represent a winning team in the Premier League. The data pipeline involves fetching data from these APIs, transforming it, and loading it into BigQuery.

## Data Sources

Below are the API data sources integrated in this project:

### 1. API-Football (by API-Sports)

* **Documentation:** [https://www.api-football.com/documentation-v3#tag/Teams/operation/get-teams-statistics](https://www.api-football.com/documentation-v3#tag/Teams/operation/get-teams-statistics)
* **Purpose:** Provides comprehensive football statistics for teams, including form, fixture summaries, goal statistics, lineups, and cards. This is a primary source for team performance metrics.
* **Endpoint Example Used:** `GET /teams/statistics`
* **Key Inputs Used in This Project:**
    * `x-rapidapi-key` (Header): Your API authentication key for api-sports.io.
    * `league` (Query Param): League ID (e.g., "39" for Premier League).
    * `season` (Query Param): Season year (e.g., "2023").
    * `team` (Query Param): Team ID (e.g., "33" for Manchester United).
* **Expected Data Output (JSON Summary):**
    * `league`: Object containing league ID, name, country, logo, flag, and season.
    * `team`: Object containing team ID, name, and logo.
    * `form`: String representing recent match outcomes (e.g., "WLWLL...").
    * `fixtures`: Object detailing matches played, wins, draws, and losses (broken down by home/away/total).
    * `goals`: Object detailing goals for and against, including totals, averages, and detailed breakdowns by minute segments (e.g., "0-15", "16-30") and over/under stats.
    * `biggest`: Object detailing streak information, biggest wins/losses, and goal margins.
    * `clean_sheet`: Object with home, away, and total clean sheets.
    * `failed_to_score`: Object with home, away, and total matches failed to score.
    * `penalty`: Object with penalty statistics (scored, missed, total).
    * `lineups`: Array of objects, each specifying a formation used and how many times it was played.
    * `cards`: Object detailing yellow and red cards, with breakdowns by minute segments.

    *(For the actual data structure, refer to the `TeamSeasonStats.csv` output or the `TeamSeasonStats` table schema in BigQuery, where complex nested parts are stored as JSON strings reflecting this structure or as specific array structures.)*

### 2. Football-Data.org API

* **Documentation:** [https://www.football-data.org/documentation/quickstart](https://www.football-data.org/documentation/quickstart)
* **Purpose:** Provides data on football competitions, matches (schedules, results, scores), teams, and standings. This is a primary source for match outcomes.
* **Endpoint Example Used:** `GET /v4/competitions/{competitionId}/matches`
* **Key Inputs Used in This Project:**
    * `X-Auth-Token` (Header): Your API authentication token for football-data.org.
    * `competition_code` (Path Param, e.g., "PL" for Premier League, used to form the URL).
    * `season` (Query Param): The year the season started (e.g., "2023" for the 2023/2024 season).
* **Expected Data Output (JSON Summary for a Match):**
    * `area`: Object with area (country) details (ID, name, code, flag).
    * `competition`: Object with competition details (ID, name, code, type, emblem).
    * `season`: Object with season details (ID, start/end dates, current matchday).
    * `id`: Unique match identifier.
    * `utcDate`: Match date and time in UTC.
    * `status`: Match status (e.g., "FINISHED", "SCHEDULED").
    * `matchday`: The matchday number in the season.
    * `stage`: Stage of the competition (e.g., "REGULAR_SEASON").
    * `homeTeam`: Object with home team details (ID, name, short name, TLA, crest).
    * `awayTeam`: Object with away team details (ID, name, short name, TLA, crest).
    * `score`: Object detailing the match score, including winner, duration, full-time, and half-time scores.
    * `referees`: Array of objects, each detailing a referee (ID, name, type, nationality).

    *(For the actual data structure, refer to the `Matches.csv` output or the `Matches` table schema in BigQuery, where complex nested parts are stored as JSON strings.)*

### 3. Sports Game Odds API (sportsgameodds.com)

* **Documentation:** [https://sportsgameodds.apidocumentation.com/reference](https://sportsgameodds.apidocumentation.com/reference)
* **Purpose:** Provides betting odds for various sports events, including soccer. This is used to gather pre-match odds and potentially player-specific betting market data.
* **Endpoint Example Used:** `GET /v2/events/`
* **Key Inputs Used in This Project:**
    * `apiKey` (Query Param): Your API authentication key.
    * `leagueID` (Query Param): Identifier for the league (e.g., "EPL" for English Premier League).
    * `startsAfter` (Query Param): Start date for events filter (YYYY-MM-DD).
    * `startsBefore` (Query Param): End date for events filter (YYYY-MM-DD).
* **Expected Data Output (JSON Summary for an Event):**
    * `eventID`: Unique identifier for the match/event.
    * `sportID`, `leagueID`, `type` (e.g., "match").
    * `info`: Object with event details (e.g., `seasonWeek`).
    * `players`: Object containing details for players involved in the event, keyed by dynamic player identifiers.
    * `teams`: Object containing `home` and `away` team details (names, team IDs for this API, colors, scores from this API's perspective if available).
    * `results`: Complex object containing game statistics broken down by period (`1h`, `2h`, `reg`, `game`) for both home/away teams, and also individual player statistics within `results.game` keyed by dynamic player identifiers.
    * `odds`: Complex object where each key is a unique odds market identifier (e.g., "points-away-1h-ou-under", "combinedCards-PLAYER_ID-game-yn-yes"). Each value is an object containing market details, odds values (`fairOdds`, `bookOdds`, lines like `bookOverUnder`), and a nested `byBookmaker` object which itself can contain odds from specific bookmakers.

    *(For the actual data structure, refer to the `MatchEventsAndOdds.csv` output or the `MatchEventsAndOdds` table schema in BigQuery, where complex nested parts like `info`, `event_players`, `teams`, `results`, and `markets` are stored as JSON strings.)*

---