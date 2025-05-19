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