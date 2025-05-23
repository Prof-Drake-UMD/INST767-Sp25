
# NYC Transit Tracker
This data pipeline aims to determine if weather has any effect on the usage of public transportation in the biggest city in the United States. 

Unfortunatly the main inspiration for this project - NYC Congestion tolls which is claimed to have drasticly reduced traffic in downtown NYC is not reflected as most data only goes to Dec. 2024

The project pulls data from three different sources

- City of New York vehicle traffic data (https://data.cityofnewyork.us/resource/7ym2-wayt) 
- State of New York NYC MTA metro ridership (https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/data_preview)
- Weather Data from the https://open-meteo.com api.

All data considered is from 2020 or later, usually until 2024 where most of the data becomes unavailable for 2025.

## Technical Details

### Ingesting & Transforming Data
In order to inject data asynchronously multiple Google Cloud Run functions and jobs were used. 

#### NYC Traffic API
The NYC traffic API returned vehicle traffic of segments of roads as well as road class. Data was combined by day to produce one single metric for volume of traffic on a given day
A CloudRun Function was used for this API.

#### Metro Ridership API
This was the largest dataset that needed to be aggregated. Each row of the response included a station id, a payment method, number of riders, a date, and more information. Each station had multiple entries for each day due to the data format. The large amount of data that needed to be ingested and transformed required multiple containers to handle the volume of data. After experimentation two containers with 10 threads each performing calls proved to be the most efficient at gathering data without triggering a backoff from the API. 
CloudRun Jobs run inside of a Docker container were used to handle this API.

#### Weather API
The Weather data from the Open-Metro dataset was highly customizable and through the parameters sent to the API the exact data needed was able to be requested and only the formatting for the data had to be adjusted to transform it to a tabular structure for import into BigQuery.

Weather contains a weathercode that corresponds to the below:
| Code           | Description                                      |
|----------------|--------------------------------------------------|
| 0              | Clear sky                                        |
| 1, 2, 3        | Mainly clear, partly cloudy, and overcast        |
| 45, 48         | Fog and depositing rime fog                      |
| 51, 53, 55     | Drizzle: Light, moderate, and dense intensity    |
| 56, 57         | Freezing Drizzle: Light and dense intensity      |
| 61, 63, 65     | Rain: Slight, moderate and heavy intensity       |
| 66, 67         | Freezing Rain: Light and heavy intensity         |
| 71, 73, 75     | Snow fall: Slight, moderate, and heavy intensity |
| 77             | Snow grains                                      |
| 80, 81, 82     | Rain showers: Slight, moderate, and violent      |
| 85, 86         | Snow showers slight and heavy                    |
| 95 *           | Thunderstorm: Slight or moderate                 |
| 96, 99 *       | Thunderstorm with slight and heavy hail          |



A CloudRun Function was used for this API.

### Pipline Process
Note - Initial Trigger is run as a Cloud Function and will return timeout errors. It still will run the other functions & jobs
````
                /    fetch-traffic-api (Cloud Function)  \
               /                                          \
Initial ______/      fetch-weather-api (Cloud Function)    \  ⭆    Output to    ⭆   Pubsub  ⭆  transform-api-output  ⭆  PubSub ⭆  bigquery-insert   ⭆  BigQuery 
Trigger       \                                            /     Bucket Storage      Trigger       (Cloud Function)        Event     (Cloud Function)       Table
               \     cloud-metro1 (Cloud Run Job)         /  
                \    cloud-metro2 (Cloud Run Job)        /
````


### Database schema
![BigQuery Screenshot](BigQuery_Screenshot.png)

[BigQuery Commands to create Tables](create_tables.md)

[Interesting Queries](db_queries.md)
#### Weather Table
 - date - DATE
 - mean_temp - FLOAT64
 - max_temp - FLOAT64
 - min_temp - FLOAT64
 - weather_code - INT
 - precipitation_sum - FLOAT64

#### Metro Ridership Table
 - Date - DATE
 - Ridership - INT

#### Traffic 
 - date - DATE
 - vol - INT
