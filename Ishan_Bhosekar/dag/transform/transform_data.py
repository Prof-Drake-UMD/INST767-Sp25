def transform_weather_data(weather_records):
    transformed = []
    for record in weather_records:
        temp = record.get("temperature")
        humidity = record.get("humidity")

        # Categorize temperature
        if temp is not None:
            if temp < 10:
                temp_category = "Cold"
            elif temp < 25:
                temp_category = "Moderate"
            else:
                temp_category = "Hot"
        else:
            temp_category = None

        # High humidity flag
        high_humidity = 1 if humidity is not None and humidity > 80 else 0

        transformed.append({
            **record,
            "temperature_category": temp_category,
            "high_humidity_flag": high_humidity
        })

    return transformed

def transform_air_quality_data(air_quality_records):
    transformed = []
    for record in air_quality_records:
        aqi = record.get("aqi")

        # Categorize AQI
        if aqi is not None:
            if aqi <= 50:
                aqi_category = "Good"
            elif aqi <= 100:
                aqi_category = "Moderate"
            elif aqi <= 150:
                aqi_category = "Unhealthy for Sensitive Groups"
            elif aqi <= 200:
                aqi_category = "Unhealthy"
            elif aqi <= 300:
                aqi_category = "Very Unhealthy"
            else:
                aqi_category = "Hazardous"
        else:
            aqi_category = None

        transformed.append({
            **record,
            "aqi_category": aqi_category
        })

    return transformed

def transform_gdp_data(gdp_records):
    transformed = []
    previous_value = None

    for record in sorted(gdp_records, key=lambda x: x.get("year", 0)):
        current_value = record.get("value")
        growth_rate = None

        if current_value is not None and previous_value is not None:
            try:
                growth_rate = ((current_value - previous_value) / previous_value) * 100
            except ZeroDivisionError:
                growth_rate = None

        transformed.append({
            **record,
            "gdp_growth_rate": round(growth_rate, 2) if growth_rate is not None else None
        })

        if current_value is not None:
            previous_value = current_value

    return transformed
