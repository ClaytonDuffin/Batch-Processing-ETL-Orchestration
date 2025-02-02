import requests
import pandas as pd
from dotenv import load_dotenv
import os
import pickle
load_dotenv()

# TODO
# Use offset to pull 24+ hours' worth for each harvester at 1am, every day. Process sets to contain full 24 hour day prior to transforming and loading.
# Insert to PostgreSQL using ON CONFLICT DO NOTHING syntax, to avoid duplicate inserts.
# Refactor extract, transform, and load blocks to be methods, which will be passed to the operators when defining the DAG.

def harvestEIA930FormData(endpoint, errorMessage):
    
    url = f"https://api.eia.gov/v2/electricity/rto/{endpoint}/data/"
    params = {
        'frequency': 'hourly',
        'data[0]': 'value',
        'start': '2025-01-31T00',
        'end': '2025-02-01T00',
        'sort[0][column]': 'period',
        'sort[0][direction]': 'asc',
        'offset': '0',
        'length': '5000',
        'api_key': os.getenv("EIA_API_KEY")
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        del data['request']['params']['api_key']
        return data
    
    raise Exception(errorMessage)
    

def harvestHourlyNetGenerationBySourceData():
    
    return harvestEIA930FormData("fuel-type-data", "Unable to harvest data for 'Hourly Net Generation by Balancing Authority and Energy Source.'")


def harvestHourlyDemandInterchangeAndGenerationData():
    
    return harvestEIA930FormData("region-data", "Unable to harvest data for 'Hourly Demand, Day-Ahead Demand Forecast, Net Generation, and Interchange by Balancing Authority.'")


def harvestHourlyInterchangeByNeighboringBA():
    
    return harvestEIA930FormData("interchange-data", "Unable to harvest data for 'Daily Interchange Between Neighboring Balancing Authorities.'")


def harvestEIA930FormDataReferenceTables():

    url = 'https://www.eia.gov/electricity/930-content/EIA930_Reference_Tables.xlsx'
    cacheFile = 'EIA930ReferenceTablesCache.pkl'
    lastModifiedTimeFile = 'LastModifiedTime.txt'

    lastModifiedHeader = requests.head(url).headers.get('Last-Modified')

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


def cleanHourlyData(hourlyData,
                    hourlyEIA930FormDataReferenceTables):
    
    filteredData = (pd.DataFrame(hourlyData['response']['data'])
                    .loc[lambda df: df['respondent' if 'respondent' in df.columns else 'fromba']
                         .isin(hourlyEIA930FormDataReferenceTables['balancingAuthorities']['BA Code'])]
                    .reset_index(drop=True))

    return filteredData


def computeHourlyNetGenerationByEnergySource(cleanedData):

    aggregatedEnergySourceData = (cleanedData
                                  .assign(value=pd.to_numeric(cleanedData['value'], errors='coerce'))
                                  .groupby(['period', 'fueltype'], as_index=False)['value']
                                  .sum()
                                  .sort_values(by=['period', 'fueltype']))

    return aggregatedEnergySourceData


def computeHourlyRespondentsProducingAndGenerating(cleanedData):
    
    hourlyRespondentsProducingAndGenerating = (cleanedData
                                               .assign(value=pd.to_numeric(cleanedData['value'], errors='coerce'))
                                               .groupby(['period', 'respondent', 'respondent-name', 'type'], as_index=False)['value']
                                               .sum()
                                               .pivot_table(index=['period', 'respondent', 'respondent-name'], columns='type', values='value', aggfunc='sum')
                                               .dropna()
                                               .reset_index()
                                               .sort_values(by=['period', 'respondent']))

    return hourlyRespondentsProducingAndGenerating


def computeHourlyStatsByResponseType(cleanedData):
    
    aggregatedResponseTypeData = (cleanedData
                                  .assign(value=pd.to_numeric(cleanedData['value'], errors='coerce'))
                                  .groupby(['period', 'type'], as_index=False)['value']
                                  .sum()
                                  .pivot_table(index='period', columns='type', values='value', aggfunc='sum')
                                  .reset_index())

    return aggregatedResponseTypeData


# extract
hourlyNetGenerationData = harvestHourlyNetGenerationBySourceData()
hourlyDemandInterchangeAndGenerationData = harvestHourlyDemandInterchangeAndGenerationData()
hourlyInterchangeByNeighboringBA = harvestHourlyInterchangeByNeighboringBA()
hourlyEIA930FormDataReferenceTables = harvestEIA930FormDataReferenceTables()

# transform
cleanedHourlyNetGenerationData = cleanHourlyData(hourlyNetGenerationData, hourlyEIA930FormDataReferenceTables)
cleanedHourlyDemandInterchangeAndGenerationData = cleanHourlyData(hourlyDemandInterchangeAndGenerationData, hourlyEIA930FormDataReferenceTables)
cleanedHourlyInterchangeByNeighboringBA = cleanHourlyData(hourlyInterchangeByNeighboringBA, hourlyEIA930FormDataReferenceTables)

transformedHourlyNetGenerationByEnergySource = computeHourlyNetGenerationByEnergySource(cleanedHourlyNetGenerationData)
transformedHourlyRespondentsProducingAndGenerating = computeHourlyRespondentsProducingAndGenerating(cleanedHourlyDemandInterchangeAndGenerationData)
transformedHourlyStatsByResponseType = computeHourlyStatsByResponseType(cleanedHourlyDemandInterchangeAndGenerationData)

# load


