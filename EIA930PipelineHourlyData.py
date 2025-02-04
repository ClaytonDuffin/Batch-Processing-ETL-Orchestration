import os
import requests
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import pickle
import psycopg2
from psycopg2.extras import execute_values
load_dotenv()

# TODO
# Refactor extract, transform, and load blocks to be methods, which will be passed to the operators when defining the DAG. DAG runs once per day.

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
    twoDaysAgo = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT00')
    
    params = {
        'frequency': 'hourly',
        'data[0]': 'value',
        'start': twoDaysAgo,
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

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT00')
    allCalls = []
    offset = 0
    
    while True:
        try:
            dataJSON = harvestEIA930FormData(endpoint, errorMessage, offset)
            allCalls.append(dataJSON)
            
            if dataJSON['response']['data'][-1]['period'] > yesterday:
                break
            
            if len(dataJSON['response']['data']) == 0:
                break
        
            offset += 5000
        
        except Exception as e:
            raise Exception(f"Error occurred for offset {offset}, endpoint {endpoint}, in paginationCycler: {e}")

    return allCalls


def cleanHourlyData(hourlyData, hourlyEIA930FormDataReferenceTables):
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT00')
    
    combinedData = (pd.concat([pd.DataFrame(entry['response']['data']) for entry in hourlyData], ignore_index=True))
    combinedData['period'] = pd.to_datetime(combinedData['period'], errors='coerce')
    dataSubset = combinedData.iloc[:combinedData[combinedData['period'].dt.strftime('%Y-%m-%dT00') == yesterday].index[0] + 1][:-1]
    
    filteredData = (dataSubset
                    .pipe(lambda df: df[df['respondent' if 'respondent' in df.columns else 'fromba']
                                        .isin(hourlyEIA930FormDataReferenceTables['balancingAuthorities']['BA Code'])]).reset_index(drop=True))

    return filteredData


def computeHourlyNetGenerationByEnergySource(cleanedData):
    
    aggregatedEnergySourceData = (cleanedData
                                  .pipe(lambda df: df.assign(value=pd.to_numeric(df['value'], errors='coerce')))
                                  .pipe(lambda df: df.groupby(['period', 'fueltype'], as_index=False)['value'].sum())
                                  .pipe(lambda df: df.sort_values(by=['period', 'fueltype'])))
    
    return aggregatedEnergySourceData


def computeHourlyRespondentsProducingAndGenerating(cleanedData):
    
    hourlyRespondentsProducingAndGenerating = (cleanedData
                                               .pipe(lambda df: df.assign(value=pd.to_numeric(df['value'], errors='coerce')))
                                               .pipe(lambda df: df.groupby(['period', 'respondent', 'respondent-name', 'type'], as_index=False)['value'].sum())
                                               .pipe(lambda df: df.pivot_table(index=['period', 'respondent', 'respondent-name'], columns='type', values='value', aggfunc='sum'))
                                               .pipe(lambda df: df.dropna())
                                               .pipe(lambda df: df.reset_index())
                                               .pipe(lambda df: df.sort_values(by=['period', 'respondent'])))
    
    return hourlyRespondentsProducingAndGenerating


def computeHourlyStatsByResponseType(cleanedData):
    
    aggregatedResponseTypeData = (cleanedData
                                  .pipe(lambda df: df.assign(value=pd.to_numeric(df['value'], errors='coerce')))
                                  .pipe(lambda df: df.groupby(['period', 'type'], as_index=False)['value'].sum())
                                  .pipe(lambda df: df.pivot_table(index='period', columns='type', values='value', aggfunc='sum'))
                                  .pipe(lambda df: df.reset_index()))

    return aggregatedResponseTypeData


def loadToPostgreSQL(tableName, transformedData):
    
    columnNames = transformedData.columns.tolist()

    try:
        connection = psycopg2.connect(
            dbname='energy_and_weather_data',
            host='localhost',
            port='5432')
        
        cursor = connection.cursor()

        upsertQuery = f"""
        INSERT INTO {tableName} ({', '.join(columnNames)})
        VALUES %s
        ON CONFLICT (period) DO NOTHING;
        """

        values = [tuple(row) for row in transformedData[columnNames].values]
        execute_values(cursor, upsertQuery, values)
        connection.commit()

    except Exception as e:
        raise Exception(f"Error occurred for table {tableName}, while loading {transformedData}, in loadToPostgreSQL: {e}")
    
    finally:
        cursor.close()
        connection.close()


# extract
hourlyEIA930FormDataReferenceTables = harvestEIA930FormDataReferenceTables()
hourlyNetGenerationData = paginationCycler("fuel-type-data", "Unable to harvest data for 'Hourly Net Generation by Balancing Authority and Energy Source.'")
hourlyDemandInterchangeAndGenerationData = paginationCycler("region-data", "Unable to harvest data for 'Hourly Demand, Day-Ahead Demand Forecast, Net Generation, and Interchange by Balancing Authority.'")
hourlyInterchangeByNeighboringBA = paginationCycler("interchange-data", "Unable to harvest data for 'Daily Interchange Between Neighboring Balancing Authorities.'")

# transform
cleanedHourlyNetGenerationData = cleanHourlyData(hourlyNetGenerationData, hourlyEIA930FormDataReferenceTables)
cleanedHourlyDemandInterchangeAndGenerationData = cleanHourlyData(hourlyDemandInterchangeAndGenerationData, hourlyEIA930FormDataReferenceTables)
cleanedHourlyInterchangeByNeighboringBA = cleanHourlyData(hourlyInterchangeByNeighboringBA, hourlyEIA930FormDataReferenceTables)

transformedHourlyNetGenerationByEnergySource = computeHourlyNetGenerationByEnergySource(cleanedHourlyNetGenerationData)
transformedHourlyRespondentsProducingAndGenerating = computeHourlyRespondentsProducingAndGenerating(cleanedHourlyDemandInterchangeAndGenerationData)
transformedHourlyStatsByResponseType = computeHourlyStatsByResponseType(cleanedHourlyDemandInterchangeAndGenerationData)

# load
loadToPostgreSQL('EIA930_hourly_statistics_by_response_type', transformedHourlyStatsByResponseType)

