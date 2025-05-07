# Fitness & Health Tracker Insights Pipeline

## API Data Sources

### Strava API
- **Inputs:** OAuth2 access token, user ID, activity endpoint
- **Outputs:** JSON with activity details (distance, calories, type, time, etc.)

### Nutritionix API
- **Inputs:** Food search string or natural language query
- **Outputs:** JSON with food name, calories, macronutrients

### OpenWeatherMap API
- **Inputs:** City name or coordinates
- **Outputs:** JSON with weather details (temperature, humidity, precipitation)

## Project Description
This project integrates fitness, nutrition, and weather data to analyze trends in exercise and health behaviors. The goal is to understand correlations such as how weather influences outdoor activity or how food intake aligns with physical exertion.
