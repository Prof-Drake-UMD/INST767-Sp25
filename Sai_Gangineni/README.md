# Fitness & Health Tracker Insights Pipeline

## API Data Sources

### OpenFitness (API Ninjas) API
- **Inputs:** Activity name (e.g., "running"), user weight (in kg)
- **Outputs:** JSON with estimated calories burned for the activity
- **Authentication:** API Key (passed in header)

### Nutritionix API
- **Inputs:** Food search string or natural language query (e.g., "1 apple")
- **Outputs:** JSON with food name, calories, macronutrients
- **Authentication:** App ID and App Key (passed in header)

### OpenWeatherMap API
- **Inputs:** City name or coordinates
- **Outputs:** JSON with weather details (temperature, humidity, precipitation)
- **Authentication:** API Key (passed in query parameter)

## Project Description
This project integrates fitness, nutrition, and weather data to analyze trends in exercise and health behaviors. The goal is to explore correlations such as how weather influences outdoor activity, or how food intake aligns with physical exertion.

The system will pull data from the three APIs, transform and clean the data, and load it into BigQuery for analysis. Example queries include:
- Average calories burned per activity type by weather condition
- Comparison of calorie intake on workout vs. non-workout days
- Analysis of which activities burn the most calories in different weather

