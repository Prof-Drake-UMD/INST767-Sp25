# Fitness & Health Tracker Insights Pipeline

## API Data Sources

### Wger API
- **Inputs:** None required for basic exercise info queries; optional query params (e.g., language filter)
- **Outputs:** JSON with details about exercises, categories, muscles, and equipment
- **Authentication:** No authentication required (public GET endpoints)

### Nutritionix API
- **Inputs:** Food search string or natural language query (e.g., "1 apple")
- **Outputs:** JSON with food name, calories, macronutrients
- **Authentication:** App ID and App Key (passed in header)

### OpenWeatherMap API
- **Inputs:** City name or coordinates
- **Outputs:** JSON with weather details (temperature, humidity, precipitation)
- **Authentication:** API Key (passed in query parameter)

## Project Description
This project integrates fitness, nutrition, and weather data to analyze trends in exercise and health behaviors. The goal is to explore correlations such as how weather influences physical activity choices, or how food intake aligns with fitness routines.

The system will pull data from the three APIs, transform and clean the data, and load it into BigQuery for analysis. Example queries include:
- Frequency of different exercise types performed under varying weather conditions
- Correlation between specific exercises and average nutritional intake
- Most popular equipment used in exercises by category
