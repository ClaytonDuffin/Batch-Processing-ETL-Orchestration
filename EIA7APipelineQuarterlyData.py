import os
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
load_dotenv()

# TODO
# Clean and restructure data.
# Identify and implement transformations.
# Initialize PostgreSQL tables. Insert to tables.
# Define DAG and components following the same structure as the other pipelines. Extract data at the midpoint of each quarter.

def harvestMSH70002AndEIA7aFormData(endpoint, errorMessage, offset):

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
            dataJSON = harvestMSH70002AndEIA7aFormData(endpoint, errorMessage, offset)
            allCalls.append(dataJSON)

            if len(dataJSON['response']['data']) == 0:
                break

            if dataJSON['response']['data'][-1]['period'] != twoQuartersAgo:
                break

            offset += 5000

        except Exception as e:
            raise Exception(f"Error occurred for offset {offset}, endpoint {endpoint}, in paginationCycler: {e}")
            
    return allCalls


# extract
monthlyCoalImportsAndExports = paginationCycler('exports-imports-quantity-price', "Unable to harvest data for 'Coal Imports and Exports (Including Price, Quantity, Country, Rank, and Customs District).'")
monthlyCoalShipmentReceipts = paginationCycler('shipments/receipts', "Unable to harvest data for 'Coal Shipment Receipts (Detailed by Transportation Type, Supplier, Mine, Coal Basin, County, State, Rank, Contract Type, Price, Quantity, and Quality).'")

# transform


# load

