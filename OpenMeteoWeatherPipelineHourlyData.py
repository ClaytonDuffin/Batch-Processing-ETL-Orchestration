from datetime import datetime, timedelta
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

# TODO
# Add remaining 26 weather variables to parameters.
# Finish cleaning, restructuring, and sorting responses.
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
    
    curatedCoordinates = pd.read_csv("https://raw.githubusercontent.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/main/curatedCoordinates.csv")[0:3]
    sevenDaysAgo = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    sixDaysAgo = (datetime.utcnow() - timedelta(days=6)).strftime("%Y-%m-%d")

    responses = []
    for _, location in curatedCoordinates.iterrows():
        response = harvestWeatherData(location['Latitude'], location['Longitude'], sevenDaysAgo, sixDaysAgo)
        responses.extend(response)
    
    return responses
    

def cleaner(weatherAtCoordinates):
    
    numberOfWeatherVariables = 4
    weatherData = []
    for location in weatherAtCoordinates:
        
        weatherAtLocation  = pd.DataFrame({"date": pd.date_range(start=pd.to_datetime(location.Hourly().Time(), unit="s", utc=True),
                                                                 end=pd.to_datetime(location.Hourly().TimeEnd(), unit="s", utc=True),
                                                                 freq=pd.Timedelta(seconds=location.Hourly().Interval()),
                                                                 inclusive="left").strftime("%Y-%m-%dT%H"),
                                           "latitude": location.Latitude(),
                                           "longitude": location.Longitude()})
        
        for variable in range(numberOfWeatherVariables):
            weatherAtLocation[variable] = location.Hourly().Variables(variable).ValuesAsNumpy()
        
        weatherData.append(weatherAtLocation)
    
    return weatherData

    
#extract
weatherAtCoordinates = coordinateCycler()

#transform 
cleanedWeatherAtCoordinates = cleaner(weatherAtCoordinates)
