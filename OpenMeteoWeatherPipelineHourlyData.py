from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# TODO
# Add remaining 26 weather variables to parameters.
# Clean, restructure, and sort responses.
# Identify and implement transformations.
# Initialize PostgreSQL tables. Insert to tables.
# Define DAG and components following the same flow as the first pipeline.

def harvestWeatherData(latitude, longitude, startDate, endDate):
    
    try:       
        if not hasattr(harvestWeatherData, 'openMeteoClient'):
            cacheSession = requests_cache.CachedSession('.cache', expire_after=-1)
            retrySession = retry(cacheSession, retries=5, backoff_factor=0.2)
            harvestWeatherData.openMeteoClient = openmeteo_requests.Client(session=retrySession)
    
        openMeteo = harvestWeatherData.openMeteoClient
        url = "https://archive-api.open-meteo.com/v1/archive"
        
        weatherVariables = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature"]
        
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": startDate,
            "end_date": endDate,
            "hourly": weatherVariables
        }
        
        response = openMeteo.weather_api(url, params=params)
        
        return response

    except Exception as e:
        raise Exception(f"Error occurred for latitude {latitude}, longitude {longitude}, in harvestWeatherData: {e}")


def coordinateCycler():
    
    try:
        curatedCoordinates = pd.read_csv("https://raw.githubusercontent.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/main/curatedCoordinates.csv")
    
        sevenDaysAgo = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        sixDaysAgo = (datetime.utcnow() - timedelta(days=6)).strftime("%Y-%m-%d")
    
        responses = []
        for _, location in curatedCoordinates.iterrows():
            response = harvestWeatherData(location['Latitude'], location['Longitude'], sevenDaysAgo, sixDaysAgo)
            responses.extend(response)
        
        return responses
    
    except Exception as e:
        raise Exception(f"Unable to harvest Open-Meteo weather data. Error occurred in coordinateCycler: {e}")


weatherAtCoordinates = coordinateCycler()

for location in weatherAtCoordinates:
    medianTemperature = ((np.median(location.Hourly().Variables(0).ValuesAsNumpy()) * (9/5)) + 32)
    print(f"{location.Latitude()}, {location.Longitude()}, {medianTemperature}°F")
