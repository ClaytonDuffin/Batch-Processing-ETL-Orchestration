from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import openmeteo_requests
import requests_cache
from retry_requests import retry
import psycopg2
from psycopg2.extras import execute_values

# TODO
# Add remaining 26 weather variables to parameters.
# Define DAG and components following the same flow as the first pipeline.

def weatherVariables():
    return ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature"]


def harvestWeatherData(latitude, longitude, startDate, endDate):
    
    if not hasattr(harvestWeatherData, 'openMeteoClient'):
        cacheSession = requests_cache.CachedSession('.cache', expire_after=-1)
        retrySession = retry(cacheSession, retries=5, backoff_factor=0.2)
        harvestWeatherData.openMeteoClient = openmeteo_requests.Client(session=retrySession)

    openMeteo = harvestWeatherData.openMeteoClient
    url = "https://archive-api.open-meteo.com/v1/archive"
        
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": startDate,
        "end_date": endDate,
        "hourly": weatherVariables()
    }
    
    response = openMeteo.weather_api(url, params=params)
    
    return response


def coordinateCycler():
    
    curatedCoordinates = pd.read_csv("https://raw.githubusercontent.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/main/curatedCoordinates.csv")[0:6]
    sevenDaysAgo = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    responses = []
    
    for _, location in curatedCoordinates.iterrows():
        response = harvestWeatherData(location['Latitude'], location['Longitude'], sevenDaysAgo, sevenDaysAgo)
        responses.extend(response)
    
    return responses
    

def cleaner(weatherAtCoordinates):
    
    weatherData = []
    numberOfWeatherVariables = len(weatherVariables())
    
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

    stackedSortedWeatherData = pd.concat(weatherData, ignore_index=True).sort_values(by='date', kind='mergesort').reset_index(drop=True)
    stackedSortedWeatherData.columns = ['date','latitude','longitude'] + weatherVariables()

    return stackedSortedWeatherData
    

def computeMetricsPerStatePerHour(cleanedWeatherAtCoordinates, computationType):
        
    states = ['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida','Georgia',
              'Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine','Maryland',
              'Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire','New Jersey',
              'New Mexico','New York','North Carolina','North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina',
              'South Dakota','Tennessee','Texas','Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming']
    
    numberOfLocationsPerState = 3
    numberOfStates = int((len(cleanedWeatherAtCoordinates)/24)/numberOfLocationsPerState)

    labeledStateMetricsPerHour = (cleanedWeatherAtCoordinates
                                  .iloc[:, 3:]
                                  .groupby(cleanedWeatherAtCoordinates.index // numberOfLocationsPerState)
                                  .apply(getattr(pd.Series, computationType))
                                  .reset_index(drop=True)
                                  .pipe(lambda df: pd.concat((df.iloc[i::numberOfStates].reset_index(drop=True).assign(state=states[i]) for i in range(numberOfStates)), ignore_index=True))
                                  .assign(date=lambda df: np.tile(cleanedWeatherAtCoordinates['date'].unique(), len(df) // len(cleanedWeatherAtCoordinates['date'].unique()))[:len(df)])
                                  .reindex(columns=['date', 'state'] + [col for col in cleanedWeatherAtCoordinates.columns if col not in ['date', 'state']])
                                  ).drop(columns=['latitude', 'longitude'], errors='ignore').sort_values(by='date', kind='mergesort').reset_index(drop=True)
    
    return labeledStateMetricsPerHour


def loadToPostgreSQL(tableName, transformedData):
    
    columnNames = transformedData.columns.tolist()

    try:
        connection = psycopg2.connect(
            dbname='energy_and_weather_data',
            host='localhost',
            port='5432')
        
        cursor = connection.cursor()

        query = f"""
        INSERT INTO {tableName} ({', '.join(columnNames)})
        VALUES %s
        ON CONFLICT (period) DO NOTHING;
        """

        values = [tuple(row) for row in transformedData[columnNames].values]
        execute_values(cursor, query, values)
        connection.commit()

    except Exception as e:
        raise Exception(f"Error occurred for table {tableName}, while loading {len(transformedData)} rows, in loadToPostgreSQL: {e}")
    
    finally:
        cursor.close()
        connection.close()


#extract
weatherAtCoordinates = coordinateCycler()

#transform 
cleanedWeatherAtCoordinates = cleaner(weatherAtCoordinates)
transformedStateMeansPerHour = computeMetricsPerStatePerHour(cleanedWeatherAtCoordinates, 'mean')
transformedStateStandardDeviationsPerHour = computeMetricsPerStatePerHour(cleanedWeatherAtCoordinates, 'std')

#load
loadToPostgreSQL('openmeteo_cleaned_weather', cleanedWeatherAtCoordinates)  
loadToPostgreSQL('openmeteo_weather_means_per_hour', transformedStateMeansPerHour)  
loadToPostgreSQL('openmeteo_weather_deviations_per_hour', transformedStateStandardDeviationsPerHour)
