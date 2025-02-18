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


def harvestEIA814FormData(offset):
    
    url = f"https://api.eia.gov/v2/crude-oil-imports/data/"
    threeMonthsAgo = (datetime.today() - relativedelta(months=3)).strftime('%Y-%m')
    
    params = {
        'frequency': 'monthly',
        'data[0]': 'quantity',
        'start': threeMonthsAgo,
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
    
    raise Exception("Unable to harvest data for 'Crude Oil Imports.'")


def paginationCycler():

    allCalls = []
    offset = 0
    
    while True:
        try:
            dataJSON = harvestEIA814FormData(offset)
            allCalls.append(dataJSON)
            
            if len(dataJSON['response']['data']) == 0:
                break
        
            offset += 5000
        
        except Exception as e:
            raise Exception(f"Error occurred for offset {offset}, in paginationCycler: {e}")

    return allCalls


def cleaner(monthlyData):
    
    combinedData = (pd.concat([pd.DataFrame(entry['response']['data']) for entry in monthlyData], ignore_index=True)).rename(columns={'period': 'date'})
    combinedData['date'] = (pd.to_datetime(combinedData['date'], errors='coerce')) + pd.offsets.MonthEnd(0)

    return combinedData


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
    monthlyCrudeOilImports = paginationCycler()  
    taskInstance.xcom_push(key='monthlyCrudeOilImports', value=monthlyCrudeOilImports)  
    
    
def transformTask(**kwargs):  

    taskInstance = kwargs['ti']  
    monthlyCrudeOilImports = taskInstance.xcom_pull(task_ids='extractTask', key='monthlyCrudeOilImports')
    cleanedMonthlyCrudeOilImports = renameColumnsToSnakeCase(cleaner(monthlyCrudeOilImports))
    taskInstance.xcom_push(key='cleanedMonthlyCrudeOilImports', value=cleanedMonthlyCrudeOilImports)  


def loadTask(**kwargs):  
    
    taskInstance = kwargs['ti']  
    cleanedMonthlyCrudeOilImports = taskInstance.xcom_pull(task_ids='transformTask', key='cleanedMonthlyCrudeOilImports')  
    loadToPostgreSQL('EIA814_cleaned_monthly_crude_oil_imports', cleanedMonthlyCrudeOilImports)


dagEIA814MonthlyData = DAG(
    'EIA814PipelineMonthlyData',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 1, 31),
        'retries': 2,
        'retry_delay': timedelta(minutes=15)},
    description='DAG to extract, transform, and load EIA-814 form monthly data. Scheduled to run once per month, on the 15th, at midnight.',
    schedule_interval='0 0 15 * *',
    catchup=False)


extract = PythonOperator(
    task_id='EIA814Extract',
    python_callable=extractTask,
    provide_context=True,
    dag=dagEIA814MonthlyData)


transform = PythonOperator(
    task_id='EIA814Transform',
    python_callable=transformTask,
    provide_context=True,
    dag=dagEIA814MonthlyData)


load = PythonOperator(
    task_id='EIA814Load',
    python_callable=loadTask,
    provide_context=True,
    dag=dagEIA814MonthlyData)


extract >> transform >> load
