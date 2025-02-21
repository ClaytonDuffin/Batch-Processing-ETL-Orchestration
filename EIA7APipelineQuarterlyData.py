import os
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python import PythonOperator
load_dotenv()


def harvestMSHA70002AndEIA7AFormData(endpoint, errorMessage, offset):

    url = f"https://api.eia.gov/v2/coal/{endpoint}/data"
                    
    params = {
        'frequency': 'quarterly',
        **({
            'data[0]': 'price',
            'data[1]': 'quantity',
        } if endpoint != 'shipments/receipts' else {
            'data[0]': 'ash-content',
            'data[1]': 'heat-content',
            'data[2]': 'price',
            'data[3]': 'quantity',
            'data[4]': 'sulfur-content'
        }),
        'sort[0][column]': 'period',
        'sort[0][direction]': 'desc',
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

    twoQuartersAgo = f"{(dt := datetime.now() - relativedelta(months=6)).year}-Q{(dt.month - 1) // 3 + 1}"
    allCalls = []
    offset = 0

    while True:
        try:
            dataJSON = harvestMSHA70002AndEIA7AFormData(endpoint, errorMessage, offset)
            allCalls.append(dataJSON)

            if len(dataJSON['response']['data']) == 0:
                break

            if dataJSON['response']['data'][-1]['period'] != twoQuartersAgo:
                break

            offset += 5000

        except Exception as e:
            raise Exception(f"Error occurred for offset {offset}, endpoint {endpoint}, in paginationCycler: {e}")
            
    return allCalls


def cleaner(quarterlyData):
    
    twoQuartersAgo = f"{(dt := datetime.now() - relativedelta(months=6)).year}-Q{(dt.month - 1) // 3 + 1}"
    
    combinedData = (pd.concat([pd.DataFrame(entry['response']['data']) for entry in quarterlyData], ignore_index=True)
                    .rename(columns={'period': 'date'})
                    .dropna(subset=['price'])
                    .loc[lambda df: pd.to_numeric(df['price'], errors='coerce').notna()]
                    .assign(date=lambda df: pd.to_datetime(df['date'], errors='coerce'))
                    .reset_index(drop=True))
        
    if 'customsDistrictId' in set(combinedData.columns):
        modifiedData = (combinedData.query('customsDistrictDescription != "Total"'))
    else:
        modifiedData = (combinedData.fillna({'mineCountyName': 'Not Specified'}))
        
    modifiedData = modifiedData[modifiedData['date'].dt.to_period('Q') == twoQuartersAgo]
    
    return modifiedData


def renameColumnsToSnakeCase(transformedDataFrame):
    
    def toSnakeCase(colName):
        colName = re.sub(r'[-\s/]+', '_', colName)
        colName = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', colName)
        return colName.lower()

    transformedDataFrame.columns = [toSnakeCase(col) for col in transformedDataFrame.columns]

    return transformedDataFrame


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
    
    quarterlyCoalImportsAndExports = paginationCycler('exports-imports-quantity-price', "Unable to harvest data for 'Coal Imports and Exports (Including Price, Quantity, Country, Rank, and Customs District).'")
    quarterlyCoalShipmentReceipts = paginationCycler('shipments/receipts', "Unable to harvest data for 'Coal Shipment Receipts (Detailed by Transportation Type, Supplier, Mine, Coal Basin, County, State, Rank, Contract Type, Price, Quantity, and Quality).'")

    taskInstance.xcom_push(key='quarterlyCoalImportsAndExports', value=quarterlyCoalImportsAndExports)  
    taskInstance.xcom_push(key='quarterlyCoalShipmentReceipts', value=quarterlyCoalShipmentReceipts)
    
    
def transformTask(**kwargs):  

    taskInstance = kwargs['ti']  
    
    quarterlyCoalImportsAndExports = taskInstance.xcom_pull(task_ids='extractTask', key='quarterlyCoalImportsAndExports')
    quarterlyCoalShipmentReceipts = taskInstance.xcom_pull(task_ids='extractTask', key='quarterlyCoalShipmentReceipts')

    cleanedQuarterlyCoalImportsAndExports = renameColumnsToSnakeCase(cleaner(quarterlyCoalImportsAndExports))
    cleanedQuarterlyCoalShipmentReceipts = renameColumnsToSnakeCase(cleaner(quarterlyCoalShipmentReceipts))

    taskInstance.xcom_push(key='cleanedQuarterlyCoalImportsAndExports', value=cleanedQuarterlyCoalImportsAndExports)
    taskInstance.xcom_push(key='cleanedQuarterlyCoalShipmentReceipts', value=cleanedQuarterlyCoalShipmentReceipts)


def loadTask(**kwargs):  
    
    taskInstance = kwargs['ti']  
    
    cleanedQuarterlyCoalImportsAndExports = taskInstance.xcom_pull(task_ids='transformTask', key='cleanedQuarterlyCoalImportsAndExports')  
    cleanedQuarterlyCoalShipmentReceipts = taskInstance.xcom_pull(task_ids='transformTask', key='cleanedQuarterlyCoalShipmentReceipts')

    loadToPostgreSQL('EIA7A_cleaned_quarterly_coal_imports_and_exports', cleanedQuarterlyCoalImportsAndExports)
    loadToPostgreSQL('EIA7A_cleaned_quarterly_coal_shipment_receipts', cleanedQuarterlyCoalShipmentReceipts)
    

dagEIA7AQuarterlyData = DAG(
    'EIA7APipelineQuarterlyData',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 31),
        'retries': 2,
        'retry_delay': timedelta(minutes=15)},
    description='DAG to extract, transform, and load EIA-7A and MSHA 7000-2 form quarterly data. Scheduled to run once per quarter, at midnight, on March 15th, June 15th, September 15th, and December 15th.',
    schedule_interval='0 0 15 3,6,9,12 *',
    catchup=False)


extract = PythonOperator(
    task_id='EIA7AExtract',
    python_callable=extractTask,
    provide_context=True,
    dag=dagEIA7AQuarterlyData)


transform = PythonOperator(
    task_id='EIA7ATransform',
    python_callable=transformTask,
    provide_context=True,
    dag=dagEIA7AQuarterlyData)


load = PythonOperator(
    task_id='EIA7ALoad',
    python_callable=loadTask,
    provide_context=True,
    dag=dagEIA7AQuarterlyData)


extract >> transform >> load
