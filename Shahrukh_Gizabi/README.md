# Soccer Data Integration Project

This project integrates data from three soccer APIs to analyze team performance statistics, primarily focusing on the Premier League. The pipeline fetches, transforms, and loads data into BigQuery for analysis.

## Data Sources

### 1. API-Football (api-sports.io)
   - **Purpose:** Detailed team statistics (form, fixtures, goals, lineups, cards).
   - **Key Inputs Used:**
     - API Key (Header: `x-rapidapi-key`)
     - League ID (e.g., "39" for Premier League)
     - Season Year (e.g., "2023")
     - Team ID (e.g., "33")
   - **Expected Key Data Output (Summarized):**
     - `league`: Basic league info.
     - `team`: Basic team info.
     - `form`: Recent match results string.
     - `fixtures`: Summary of played, wins, draws, losses.
     - `goals`: Detailed goals for/against, including minute breakdowns and over/under stats.
     - `lineups`: Array of formations played.
     - `cards`: Yellow/red card details with minute breakdowns.
   - **Documentation:** [API-Football Docs](https://www.api-football.com/documentation-v3#tag/Teams/operation/get-teams-statistics)

### 2. Football-Data.org API
   - **Purpose:** Match results, schedules, scores, and competition details.
   - **Key Inputs Used:**
     - API Token (Header: `X-Auth-Token`)
     - Competition Code (e.g., "PL" for Premier League)
     - Season Year (e.g., "2023")
   - **Expected Key Data Output (Summarized for a Match):**
     - `id`: Unique match ID.
     - `utcDate`: Match date/time.
     - `status`: Match status (e.g., FINISHED).
     - `homeTeam`, `awayTeam`: Details for each team (ID, name).
     - `score`: Winner, full-time, and half-time scores.
     - `referees`: List of match officials.
   - **Documentation:** [Football-Data.org Docs](https://www.football-data.org/documentation/quickstart)

### 3. Sports Game Odds API (sportsgameodds.com)
   - **Purpose:** Betting odds for matches, including various markets.
   - **Key Inputs Used:**
     - API Key (Query Param: `apiKey`)
     - League ID (e.g., "EPL")
     - Date Range (`startsAfter`, `startsBefore`)
   - **Expected Key Data Output (Summarized for an Event):**
     - `eventID`: Unique event ID.
     - `info`: Basic event info (e.g., `seasonWeek`).
     - `players`: List of involved players (if available).
     - `teams`: Home and away team details specific to this API.
     * `results`: In-game statistics (if available from this source), including player-specific stats.
     * `odds`: Detailed betting odds across multiple markets (e.g., moneyline, spreads, totals) from various bookmakers.
   - **Documentation:** [Sports Game Odds API Docs](https://sportsgameodds.apidocumentation.com/reference)

---