import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
import numpy as np

# TODO
# Add remaining 26 weather variables to parameters.
# Refactor coordinateCycler to pull coordinates from curatedCoordinates.csv.
# Clean, restructure, and sort responses.
# Identify and implement transformations.
# Initialize PostgreSQL tables. Insert to tables.
# Define DAG and components following the same flow as the first pipeline.

def harvestWeatherData(latitude, longitude, startDate, endDate):
    
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


def coordinateCycler():
    
    coordinates = [{"latitude": 34.922183, "longitude": -85.67849},
                   {"latitude": 32.93565, "longitude": -86.889836}]

    sevenDaysAgo = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    sixDaysAgo = (datetime.utcnow() - timedelta(days=6)).strftime("%Y-%m-%d")
    
    responses = []
    for coordinate in coordinates:
        response = harvestWeatherData(coordinate['latitude'], coordinate['longitude'], sevenDaysAgo, sixDaysAgo)
        responses.extend(response)
    
    return responses


weatherAtCoordinates = coordinateCycler()

for coordinate in weatherAtCoordinates:
    medianTemperature = ((np.median(coordinate.Hourly().Variables(0).ValuesAsNumpy()) * (9/5)) + 32)
    print(f"{coordinate.Latitude()}, {coordinate.Longitude()}, {medianTemperature}°F")

