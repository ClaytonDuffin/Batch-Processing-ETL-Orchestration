from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import openmeteo_requests
import requests_cache
from retry_requests import retry
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator


def weatherVariables():
    
    return ['temperature_2m', 'relative_humidity_2m', 'dew_point_2m', 'apparent_temperature', 'precipitation',
            'rain', 'snowfall', 'snow_depth', 'weather_code', 'pressure_msl',
            'surface_pressure', 'cloud_cover', 'cloud_cover_low', 'cloud_cover_mid', 'cloud_cover_high',
            'et0_fao_evapotranspiration', 'vapour_pressure_deficit', 'wind_speed_10m', 'wind_speed_100m', 'wind_direction_10m',
            'wind_direction_100m', 'wind_gusts_10m', 'soil_temperature_0_to_7cm', 'soil_temperature_7_to_28cm', 'soil_temperature_28_to_100cm',
            'soil_temperature_100_to_255cm', 'soil_moisture_0_to_7cm', 'soil_moisture_7_to_28cm', 'soil_moisture_28_to_100cm', 'soil_moisture_100_to_255cm']


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
    
    curatedCoordinates = pd.read_csv("https://raw.githubusercontent.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/main/curatedCoordinates.csv")
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
    stackedSortedWeatherData['date'] = pd.to_datetime(stackedSortedWeatherData['date'], format='%Y-%m-%dT%H')

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
        connection = psycopg2.connect(dbname='energy_and_weather_data', host='localhost', port='5432')
        cursor = connection.cursor()

        query = f"""
        INSERT INTO {tableName} ({', '.join(columnNames)})
        VALUES %s
        """

        values = [tuple(row) for row in transformedData[columnNames].values]
        execute_values(cursor, query, values)
        connection.commit()

    except Exception as e:
        raise Exception(f"Error occurred for table {tableName} while loading {transformedData}: {e}")

    finally:
        cursor.close()
        connection.close()


def extractTask(**kwargs):  
    
    taskInstance = kwargs['ti']  
    
    weatherAtCoordinates = coordinateCycler()  
    
    taskInstance.xcom_push(key='weatherAtCoordinates', value=weatherAtCoordinates)  
    

def transformTask(**kwargs):  

    taskInstance = kwargs['ti']  

    weatherAtCoordinates = taskInstance.xcom_pull(task_ids='extractTask', key='weatherAtCoordinates')

    cleanedWeatherAtCoordinates = cleaner(weatherAtCoordinates)  
    
    transformedStateMeansPerHour = computeMetricsPerStatePerHour(cleanedWeatherAtCoordinates, 'mean')
    transformedStateStandardDeviationsPerHour = computeMetricsPerStatePerHour(cleanedWeatherAtCoordinates, 'std')

    taskInstance.xcom_push(key='cleanedWeatherAtCoordinates', value=cleanedWeatherAtCoordinates)  
    taskInstance.xcom_push(key='transformedStateMeansPerHour', value=transformedStateMeansPerHour)  
    taskInstance.xcom_push(key='transformedStateStandardDeviationsPerHour', value=transformedStateStandardDeviationsPerHour)  


def loadTask(**kwargs):  
    
    taskInstance = kwargs['ti']  

    cleanedWeatherAtCoordinates = taskInstance.xcom_pull(task_ids='transformTask', key='cleanedWeatherAtCoordinates')  
    transformedStateMeansPerHour = taskInstance.xcom_pull(task_ids='transformTask', key='transformedStateMeansPerHour')  
    transformedStateStandardDeviationsPerHour = taskInstance.xcom_pull(task_ids='transformTask', key='transformedStateStandardDeviationsPerHour')  
   
    loadToPostgreSQL('openmeteo_cleaned_weather', cleanedWeatherAtCoordinates)  
    loadToPostgreSQL('openmeteo_weather_means_per_hour', transformedStateMeansPerHour)  
    loadToPostgreSQL('openmeteo_weather_deviations_per_hour', transformedStateStandardDeviationsPerHour)


dagOpenMeteoHourlyData = DAG(
    'OpenMeteoWeatherPipelineHourlyData',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 31),
        'retries': 2,
        'retry_delay': timedelta(minutes=15)},
    description='DAG to extract, transform, and load weather data. Scheduled to run once per day.',
    schedule_interval=timedelta(days=1),
    catchup=False)


extract = PythonOperator(
    task_id='OpenMeteoExtract',
    python_callable=extractTask,
    provide_context=True,
    dag=dagOpenMeteoHourlyData)


transform = PythonOperator(
    task_id='OpenMeteoTransform',
    python_callable=transformTask,
    provide_context=True,
    dag=dagOpenMeteoHourlyData)


load = PythonOperator(
    task_id='OpenMeteoLoad',
    python_callable=loadTask,
    provide_context=True,
    dag=dagOpenMeteoHourlyData)


extract >> transform >> load
