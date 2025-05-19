# Sports Data Integration System

This project implements data pipeline that integrates three different data sources related to sports events, media engagement, and audience search interest. The system collects data from sports events, analyzes YouTube video metrics, and correlates these with Google Trends search interest to discover insights about audience engagement patterns.

## API Integration

The project integrates three complementary data sources:

1. **Sports Event Data**: TheSportsDB provides information about leagues, teams, and events including scores and statistics

   [https://www.thesportsdb.com/api.php](https://www.thesportsdb.com/api.php)

2. **Video Performance**: YouTube Data API provides metrics on sports-related videos including views, likes, and comments

   [https://developers.google.com/youtube/v3](https://developers.google.com/youtube/v3)

3. **Audience Interest**: Google Trends API provides data on search interest for sports teams and events

   [https://github.com/GeneralMills/pytrends](https://github.com/GeneralMills/pytrends)

These data sources are combined to create a comprehensive view of how sporting events drive online engagement across multiple platforms.

## BigQuery Data Model

The project uses a BigQuery-optimized data model with the following key tables:

### Core Tables

1. **events** - Sports events data including teams, scores, and metadata
2. **teams** - Information about sports teams including social media and metadata
3. **youtube_videos** - YouTube videos related to sports events
4. **video_metrics** - Daily metrics for YouTube videos
5. **video_comments** - Comments on YouTube videos with sentiment analysis
6. **search_trends** - Google Trends data showing search interest over time
7. **regional_interest** - Geographic distribution of search interest
8. **related_queries** - Related search terms from Google Trends
9. **integrated_events_analysis** - Denormalized table combining events with engagement metrics

### Schema Design Principles

* Tables are partitioned by date for efficient time-based queries
* Key tables are clustered for performance on common filters
* Complex data is stored using nested and repeated fields (STRUCT and ARRAY types)
* The model is denormalized for analytical efficiency

### Analytical Capabilities

The data model enables complex analytical queries including:

1. Cross-platform engagement analysis for sports events
2. Regional interest patterns for teams and events
3. Content performance analysis by type and timing
4. Temporal impact analysis of events on online engagement
5. Audience sentiment analysis through comments

SQL files for both table definitions and analytical queries can be found in the `sql/` directory.