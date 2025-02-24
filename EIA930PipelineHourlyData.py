import os
import re
import pickle
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
load_dotenv()


def harvestEIA930FormDataReferenceTables():
    
    url = 'https://www.eia.gov/electricity/930-content/EIA930_Reference_Tables.xlsx'
    lastModifiedHeader = requests.head(url).headers.get('Last-Modified')
    cacheFile = 'EIA930ReferenceTablesCache.pkl'
    lastModifiedTimeFile = 'LastModifiedTime.txt'

    if not os.path.exists(lastModifiedTimeFile):
        with open(lastModifiedTimeFile, 'w') as f:
            f.write('')

    with open(lastModifiedTimeFile, 'r') as f:
        lastModifiedCached = f.read().strip()

    if lastModifiedHeader == lastModifiedCached:
        with open(cacheFile, 'rb') as f:
            return pickle.load(f)

    selectedSheets = pd.read_excel(url, sheet_name=['BAs', 'Energy Sources'])
    referenceTables = {'balancingAuthorities': selectedSheets['BAs'].iloc[:, :6], 'energySources': selectedSheets['Energy Sources']}

    with open(cacheFile, 'wb') as f:
        pickle.dump(referenceTables, f)

    with open(lastModifiedTimeFile, 'w') as f:
        f.write(lastModifiedHeader)

    return referenceTables


def harvestEIA930FormData(endpoint, errorMessage, offset):
        
    url = f"https://api.eia.gov/v2/electricity/rto/{endpoint}/data/"
    threeDaysAgo = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%dT00')
    
    params = {
        'frequency': 'hourly',
        'data[0]': 'value',
        'start': threeDaysAgo,
        'sort[0][column]': 'period',
        'sort[0][direction]': 'asc',
        'offset': offset,
        'length': '5000',
        'api_key': os.getenv("EIA_API_KEY")
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        del data['request']['params']['api_key']
        return data
    
    raise Exception(errorMessage)


def paginationCycler(endpoint, errorMessage):

    twoDaysAgo = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT00')
    allCalls = []
    offset = 0
    
    while True:
        try:
            dataJSON = harvestEIA930FormData(endpoint, errorMessage, offset)
            allCalls.append(dataJSON)
            
            if len(dataJSON['response']['data']) == 0:
                break
            
            if dataJSON['response']['data'][-1]['period'] > twoDaysAgo:
                break
        
            offset += 5000
        
        except Exception as e:
            raise Exception(f"Error occurred for offset {offset}, endpoint {endpoint}, in paginationCycler: {e}")

    return allCalls


def cleaner(hourlyData, hourlyEIA930FormDataReferenceTables):
    
    twoDaysAgo = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT00')
    
    combinedData = (pd.concat([pd.DataFrame(entry['response']['data']) for entry in hourlyData], ignore_index=True)).rename(columns={'period': 'date'})
    combinedData['date'] = pd.to_datetime(combinedData['date'], errors='coerce')
    dataSubset = combinedData.iloc[:combinedData[combinedData['date'].dt.strftime('%Y-%m-%dT%H') == twoDaysAgo].index[0] + 1][:-1]
    
    filteredData = (dataSubset
                    .pipe(lambda df: df[df['respondent' if 'respondent' in df.columns else 'fromba']
                                        .isin(hourlyEIA930FormDataReferenceTables['balancingAuthorities']['BA Code'])]).reset_index(drop=True))

    return filteredData


def computeHourlyNetGenerationByEnergySource(cleanedData):
    
    aggregatedEnergySourceData = (cleanedData
                                  .pipe(lambda df: df.assign(value=pd.to_numeric(df['value'], errors='coerce')))
                                  .pipe(lambda df: df.groupby(['date', 'fueltype'], as_index=False)['value'].sum())
                                  .pipe(lambda df: df.sort_values(by=['date', 'fueltype'])))
    
    return aggregatedEnergySourceData


def computeHourlyRespondentsProducingAndGenerating(cleanedData):
    
    hourlyRespondentsProducingAndGenerating = (cleanedData
                                               .pipe(lambda df: df.assign(value=pd.to_numeric(df['value'], errors='coerce')))
                                               .pipe(lambda df: df.groupby(['date', 'respondent', 'respondent-name', 'type'], as_index=False)['value'].sum())
                                               .pipe(lambda df: df.pivot_table(index=['date', 'respondent', 'respondent-name'], columns='type', values='value', aggfunc='sum'))
                                               .pipe(lambda df: df.dropna())
                                               .pipe(lambda df: df.reset_index())
                                               .pipe(lambda df: df.sort_values(by=['date', 'respondent'])))
    
    return hourlyRespondentsProducingAndGenerating


def computeHourlyStatsByResponseType(cleanedData):
    
    aggregatedResponseTypeData = (cleanedData
                                  .pipe(lambda df: df.assign(value=pd.to_numeric(df['value'], errors='coerce')))
                                  .pipe(lambda df: df.groupby(['date', 'type'], as_index=False)['value'].sum())
                                  .pipe(lambda df: df.pivot_table(index='date', columns='type', values='value', aggfunc='sum'))
                                  .pipe(lambda df: df.reset_index()))

    return aggregatedResponseTypeData


def renameColumnsToSnakeCase(*transformedDataFrames):
    
    def toSnakeCase(colName):
        colName = re.sub(r'[-\s/]+', '_', colName)
        colName = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', colName)
        return colName.lower()

    for transformedData in transformedDataFrames:
        transformedData.columns = [toSnakeCase(col) for col in transformedData.columns]

    return transformedDataFrames


def loadToPostgreSQL(tableName, transformedData):

    columnNames = transformedData.columns.tolist()

    try:
        connection = psycopg2.connect(dbname="energy_and_weather_data", host='postgres', port='5432', user='airflow', password='airflow')
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


def serializationProcess(dataObject):
    
    serializedData = pickle.dumps(dataObject).hex()
    
    return serializedData
    

def deserializationProcess(serializedData):
    
    dataObject = pickle.loads(bytes.fromhex(serializedData))
    
    return dataObject


def extractTask(**kwargs):  
    
    taskInstance = kwargs['ti']  

    hourlyEIA930FormDataReferenceTables = serializationProcess(harvestEIA930FormDataReferenceTables())
    hourlyNetGenerationData = serializationProcess(paginationCycler("fuel-type-data", "Unable to harvest data for 'Hourly Net Generation by Balancing Authority and Energy Source.'"))  
    hourlyDemandInterchangeAndGenerationData = serializationProcess(paginationCycler("region-data", "Unable to harvest data for 'Hourly Demand, Day-Ahead Demand Forecast, Net Generation, and Interchange by Balancing Authority.'"))  
    hourlyInterchangeByNeighboringBA = serializationProcess(paginationCycler("interchange-data", "Unable to harvest data for 'Daily Interchange Between Neighboring Balancing Authorities.'"))

    taskInstance.xcom_push(key='hourlyEIA930FormDataReferenceTables', value=hourlyEIA930FormDataReferenceTables)  
    taskInstance.xcom_push(key='hourlyNetGenerationData', value=hourlyNetGenerationData)  
    taskInstance.xcom_push(key='hourlyDemandInterchangeAndGenerationData', value=hourlyDemandInterchangeAndGenerationData)  
    taskInstance.xcom_push(key='hourlyInterchangeByNeighboringBA', value=hourlyInterchangeByNeighboringBA)  


def transformTask(**kwargs):  

    taskInstance = kwargs['ti']  

    hourlyEIA930FormDataReferenceTables = taskInstance.xcom_pull(task_ids='EIA930Extract', key='hourlyEIA930FormDataReferenceTables')  
    hourlyNetGenerationData = taskInstance.xcom_pull(task_ids='EIA930Extract', key='hourlyNetGenerationData')  
    hourlyDemandInterchangeAndGenerationData = taskInstance.xcom_pull(task_ids='EIA930Extract', key='hourlyDemandInterchangeAndGenerationData')  
    hourlyInterchangeByNeighboringBA = taskInstance.xcom_pull(task_ids='EIA930Extract', key='hourlyInterchangeByNeighboringBA')  
    
    hourlyEIA930FormDataReferenceTables = deserializationProcess(hourlyEIA930FormDataReferenceTables)

    cleanedHourlyNetGenerationData = cleaner(deserializationProcess(hourlyNetGenerationData), hourlyEIA930FormDataReferenceTables)
    cleanedHourlyDemandInterchangeAndGenerationData = cleaner(deserializationProcess(hourlyDemandInterchangeAndGenerationData), hourlyEIA930FormDataReferenceTables) 
    cleanedHourlyInterchangeByNeighboringBA = cleaner(deserializationProcess(hourlyInterchangeByNeighboringBA), hourlyEIA930FormDataReferenceTables)

    transformedHourlyNetGenerationByEnergySource = computeHourlyNetGenerationByEnergySource(cleanedHourlyNetGenerationData)  
    transformedHourlyRespondentsProducingAndGenerating = computeHourlyRespondentsProducingAndGenerating(cleanedHourlyDemandInterchangeAndGenerationData)  
    transformedHourlyStatsByResponseType = computeHourlyStatsByResponseType(cleanedHourlyDemandInterchangeAndGenerationData)  

    balancingAuthorities, energySources = renameColumnsToSnakeCase(  
        hourlyEIA930FormDataReferenceTables['balancingAuthorities'],  
        hourlyEIA930FormDataReferenceTables['energySources'])  

    cleanedHourlyNetGenerationData, cleanedHourlyDemandInterchangeAndGenerationData, cleanedHourlyInterchangeByNeighboringBA = renameColumnsToSnakeCase(  
        cleanedHourlyNetGenerationData,  
        cleanedHourlyDemandInterchangeAndGenerationData,  
        cleanedHourlyInterchangeByNeighboringBA)  

    transformedHourlyNetGenerationByEnergySource, transformedHourlyRespondentsProducingAndGenerating, transformedHourlyStatsByResponseType = renameColumnsToSnakeCase(  
        transformedHourlyNetGenerationByEnergySource,  
        transformedHourlyRespondentsProducingAndGenerating,  
        transformedHourlyStatsByResponseType)  
    
    balancingAuthorities = serializationProcess(balancingAuthorities)
    energySources = serializationProcess(energySources)
    cleanedHourlyNetGenerationData = serializationProcess(cleanedHourlyNetGenerationData)
    cleanedHourlyDemandInterchangeAndGenerationData = serializationProcess(cleanedHourlyDemandInterchangeAndGenerationData)
    cleanedHourlyInterchangeByNeighboringBA = serializationProcess(cleanedHourlyInterchangeByNeighboringBA)
    transformedHourlyNetGenerationByEnergySource = serializationProcess(transformedHourlyNetGenerationByEnergySource)
    transformedHourlyRespondentsProducingAndGenerating = serializationProcess(transformedHourlyRespondentsProducingAndGenerating)
    transformedHourlyStatsByResponseType = serializationProcess(transformedHourlyStatsByResponseType)

    taskInstance.xcom_push(key='balancingAuthorities', value=balancingAuthorities)  
    taskInstance.xcom_push(key='energySources', value=energySources)  
    taskInstance.xcom_push(key='cleanedHourlyNetGenerationData', value=cleanedHourlyNetGenerationData)  
    taskInstance.xcom_push(key='cleanedHourlyDemandInterchangeAndGenerationData', value=cleanedHourlyDemandInterchangeAndGenerationData)  
    taskInstance.xcom_push(key='cleanedHourlyInterchangeByNeighboringBA', value=cleanedHourlyInterchangeByNeighboringBA)  
    taskInstance.xcom_push(key='transformedHourlyNetGenerationByEnergySource', value=transformedHourlyNetGenerationByEnergySource)  
    taskInstance.xcom_push(key='transformedHourlyRespondentsProducingAndGenerating', value=transformedHourlyRespondentsProducingAndGenerating)  
    taskInstance.xcom_push(key='transformedHourlyStatsByResponseType', value=transformedHourlyStatsByResponseType)  


def loadTask(**kwargs):  
    
    taskInstance = kwargs['ti']  

    balancingAuthorities = taskInstance.xcom_pull(task_ids='EIA930Transform', key='balancingAuthorities')  
    energySources = taskInstance.xcom_pull(task_ids='EIA930Transform', key='energySources')  
    cleanedHourlyNetGenerationData = taskInstance.xcom_pull(task_ids='EIA930Transform', key='cleanedHourlyNetGenerationData')  
    cleanedHourlyDemandInterchangeAndGenerationData = taskInstance.xcom_pull(task_ids='EIA930Transform', key='cleanedHourlyDemandInterchangeAndGenerationData')  
    cleanedHourlyInterchangeByNeighboringBA = taskInstance.xcom_pull(task_ids='EIA930Transform', key='cleanedHourlyInterchangeByNeighboringBA')  
    transformedHourlyNetGenerationByEnergySource = taskInstance.xcom_pull(task_ids='EIA930Transform', key='transformedHourlyNetGenerationByEnergySource')  
    transformedHourlyRespondentsProducingAndGenerating = taskInstance.xcom_pull(task_ids='EIA930Transform', key='transformedHourlyRespondentsProducingAndGenerating')  
    transformedHourlyStatsByResponseType = taskInstance.xcom_pull(task_ids='EIA930Transform', key='transformedHourlyStatsByResponseType')  

    loadToPostgreSQL('EIA930_balancing_authorities', deserializationProcess(balancingAuthorities))  
    loadToPostgreSQL('EIA930_energy_sources', deserializationProcess(energySources))  
    loadToPostgreSQL('EIA930_cleaned_hourly_net_generation', deserializationProcess(cleanedHourlyNetGenerationData))  
    loadToPostgreSQL('EIA930_cleaned_hourly_demand_interchange_generation', deserializationProcess(cleanedHourlyDemandInterchangeAndGenerationData))  
    loadToPostgreSQL('EIA930_cleaned_hourly_interchange_by_neighboring_BA', deserializationProcess(cleanedHourlyInterchangeByNeighboringBA))  
    loadToPostgreSQL('EIA930_hourly_net_generation_by_energy_source', deserializationProcess(transformedHourlyNetGenerationByEnergySource))  
    loadToPostgreSQL('EIA930_hourly_respondents_producing_and_generating', deserializationProcess(transformedHourlyRespondentsProducingAndGenerating))  
    loadToPostgreSQL('EIA930_hourly_statistics_by_response_type', deserializationProcess(transformedHourlyStatsByResponseType))  


dagEIA930HourlyData = DAG(
    'EIA930PipelineHourlyData',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 31),
        'retries': 2,
        'retry_delay': timedelta(minutes=15)},
    description='DAG to extract, transform, and load EIA-930 form hourly data, and EIA-930 form reference tables. Scheduled to run once per day.',
    schedule_interval='0 1 * * *',
    catchup=False)


extract = PythonOperator(
    task_id='EIA930Extract',
    python_callable=extractTask,
    provide_context=True,
    dag=dagEIA930HourlyData)


transform = PythonOperator(
    task_id='EIA930Transform',
    python_callable=transformTask,
    provide_context=True,
    dag=dagEIA930HourlyData)


load = PythonOperator(
    task_id='EIA930Load',
    python_callable=loadTask,
    provide_context=True,
    dag=dagEIA930HourlyData)


extract >> transform >> load
