import psycopg2


def createDatabase(databaseName):

    with psycopg2.connect(dbname="postgres") as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (databaseName,))
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {databaseName}")
                print(f"Database '{databaseName}' created successfully!")
                cursor.close()
                connection.close()
            else:
                print(f"Database '{databaseName}' already exists.")
                cursor.close()
                connection.close()


def createTable(databaseName, tableName, tableColumnNames):
    
    connection = psycopg2.connect(dbname=databaseName, host='localhost', port='5432')
    cursor = connection.cursor()
    
    columnNames = ", ".join([f"{col} {dtype}" for col, dtype in tableColumnNames.items()])
    
    query = f"""
    CREATE TABLE IF NOT EXISTS {tableName} (
        {columnNames}
    );
    """
    
    cursor.execute(query)
    connection.commit()
    print(f"Table '{tableName}' created successfully!")
    cursor.close()
    connection.close()


def displayTableContents(databaseName, tableName, numberOfRowsToDisplay=5):
    
    connection = psycopg2.connect(dbname=databaseName, host='localhost', port='5432')
    cursor = connection.cursor()

    query = f"""
        SELECT * FROM {tableName}
        LIMIT {numberOfRowsToDisplay};
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    cursor.close()
    connection.close()


def deleteTableContents(databaseName, tableName):
    
    connection = psycopg2.connect(dbname=databaseName, host='localhost', port='5432')
    cursor = connection.cursor()

    query = f"DELETE FROM {tableName}"
    
    cursor.execute(query)
    connection.commit()

    print(f"Contents have been deleted from {tableName}.")

    cursor.close()
    connection.close()


def displayAllTableNames(databaseName):
    
    connection = psycopg2.connect(dbname=databaseName, host='localhost', port='5432')
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    tables = cursor.fetchall()
    
    for table in tables:
        print(table[0])
    
    cursor.close()
    connection.close()

        
def removeAllTablesfromDatabase(databaseName):
    
    connection = psycopg2.connect(dbname=databaseName, host='localhost', port='5432')
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public';
    """)
    
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            print(f"Table {table_name} has been removed.")
        except Exception as e:
            print(f"Error removing table {table_name}: {e}")
    
    connection.commit()
    
    cursor.close()
    connection.close()


createDatabase("energy_and_weather_data")


createTable("energy_and_weather_data",
            "EIA930_balancing_authorities", 
            {"ba_code": "TEXT",
             "ba_name": "TEXT",
             "time_zone": "TEXT",
             "region_country_code": "TEXT",
             "region_country_name": "TEXT",
             "generation_only_ba": "TEXT"})


createTable("energy_and_weather_data",
            "EIA930_energy_sources", 
            {"energy_source_code": "TEXT",
             "energy_source_name": "TEXT"})


createTable("energy_and_weather_data",
            "EIA930_cleaned_hourly_net_generation", 
            {"date": "TIMESTAMP",
             "respondent": "TEXT",
             "respondent_name": "TEXT",
             "fueltype": "TEXT",
             "type_name": "TEXT",
             "value": "FLOAT",
             "value_units": "TEXT"})


createTable("energy_and_weather_data",
            "EIA930_cleaned_hourly_demand_interchange_generation", 
            {"date": "TIMESTAMP",
             "respondent": "TEXT",
             "respondent_name": "TEXT",
             "type": "TEXT",
             "type_name": "TEXT",
             "value": "FLOAT",
             "value_units": "TEXT"})


createTable("energy_and_weather_data",
            "EIA930_cleaned_hourly_interchange_by_neighboring_BA", 
            {"date": "TIMESTAMP",
             "fromba": "TEXT",
             "fromba_name": "TEXT",
             "toba": "TEXT",
             "toba_name": "TEXT",
             "value": "FLOAT",
             "value_units": "TEXT"})


createTable("energy_and_weather_data",
            "EIA930_hourly_net_generation_by_energy_source", 
            {"date": "TIMESTAMP",
             "fueltype": "TEXT",
             "value": "FLOAT"})


createTable("energy_and_weather_data",
            "EIA930_hourly_respondents_producing_and_generating", 
            {"date": "TIMESTAMP",
             "respondent": "TEXT",
             "respondent_name": "TEXT",
             "d": "FLOAT",
             "df": "FLOAT",
             "ng": "FLOAT",
             "ti": "FLOAT"})


createTable("energy_and_weather_data",
            "EIA930_hourly_statistics_by_response_type", 
            {"date": "TIMESTAMP",
             "d": "FLOAT",
             "df": "FLOAT",
             "ng": "FLOAT",
             "ti": "FLOAT"})


createTable("energy_and_weather_data",
            "openmeteo_cleaned_weather",
            {"date": "TIMESTAMP",
             "latitude": "FLOAT",
             "longitude": "FLOAT",
             "temperature_2m": "FLOAT",
             "relative_humidity_2m": "FLOAT",
             "dew_point_2m": "FLOAT",
             "apparent_temperature": "FLOAT",
             "precipitation": "FLOAT",
             "rain": "FLOAT",
             "snowfall": "FLOAT",
             "snow_depth": "FLOAT",
             "weather_code": "FLOAT",
             "pressure_msl": "FLOAT",
             "surface_pressure": "FLOAT",
             "cloud_cover": "FLOAT",
             "cloud_cover_low": "FLOAT",
             "cloud_cover_mid": "FLOAT",
             "cloud_cover_high": "FLOAT",
             "et0_fao_evapotranspiration": "FLOAT",
             "vapour_pressure_deficit": "FLOAT",
             "wind_speed_10m": "FLOAT",
             "wind_speed_100m": "FLOAT",
             "wind_direction_10m": "FLOAT",
             "wind_direction_100m": "FLOAT",
             "wind_gusts_10m": "FLOAT",
             "soil_temperature_0_to_7cm": "FLOAT",
             "soil_temperature_7_to_28cm": "FLOAT",
             "soil_temperature_28_to_100cm": "FLOAT",
             "soil_temperature_100_to_255cm": "FLOAT",
             "soil_moisture_0_to_7cm": "FLOAT",
             "soil_moisture_7_to_28cm": "FLOAT",
             "soil_moisture_28_to_100cm": "FLOAT",
             "soil_moisture_100_to_255cm": "FLOAT"})


createTable("energy_and_weather_data",
            "openmeteo_weather_means_per_hour", 
            {"date": "TIMESTAMP",
             "state": "TEXT",
             "temperature_2m": "FLOAT",
             "relative_humidity_2m": "FLOAT",
             "dew_point_2m": "FLOAT",
             "apparent_temperature": "FLOAT",
             "precipitation": "FLOAT",
             "rain": "FLOAT",
             "snowfall": "FLOAT",
             "snow_depth": "FLOAT",
             "weather_code": "FLOAT",
             "pressure_msl": "FLOAT",
             "surface_pressure": "FLOAT",
             "cloud_cover": "FLOAT",
             "cloud_cover_low": "FLOAT",
             "cloud_cover_mid": "FLOAT",
             "cloud_cover_high": "FLOAT",
             "et0_fao_evapotranspiration": "FLOAT",
             "vapour_pressure_deficit": "FLOAT",
             "wind_speed_10m": "FLOAT",
             "wind_speed_100m": "FLOAT",
             "wind_direction_10m": "FLOAT",
             "wind_direction_100m": "FLOAT",
             "wind_gusts_10m": "FLOAT",
             "soil_temperature_0_to_7cm": "FLOAT",
             "soil_temperature_7_to_28cm": "FLOAT",
             "soil_temperature_28_to_100cm": "FLOAT",
             "soil_temperature_100_to_255cm": "FLOAT",
             "soil_moisture_0_to_7cm": "FLOAT",
             "soil_moisture_7_to_28cm": "FLOAT",
             "soil_moisture_28_to_100cm": "FLOAT",
             "soil_moisture_100_to_255cm": "FLOAT"})


createTable("energy_and_weather_data",
            "openmeteo_weather_deviations_per_hour", 
            {"date": "TIMESTAMP",
             "state": "TEXT",
             "temperature_2m": "FLOAT",
             "relative_humidity_2m": "FLOAT",
             "dew_point_2m": "FLOAT",
             "apparent_temperature": "FLOAT",
             "precipitation": "FLOAT",
             "rain": "FLOAT",
             "snowfall": "FLOAT",
             "snow_depth": "FLOAT",
             "weather_code": "FLOAT",
             "pressure_msl": "FLOAT",
             "surface_pressure": "FLOAT",
             "cloud_cover": "FLOAT",
             "cloud_cover_low": "FLOAT",
             "cloud_cover_mid": "FLOAT",
             "cloud_cover_high": "FLOAT",
             "et0_fao_evapotranspiration": "FLOAT",
             "vapour_pressure_deficit": "FLOAT",
             "wind_speed_10m": "FLOAT",
             "wind_speed_100m": "FLOAT",
             "wind_direction_10m": "FLOAT",
             "wind_direction_100m": "FLOAT",
             "wind_gusts_10m": "FLOAT",
             "soil_temperature_0_to_7cm": "FLOAT",
             "soil_temperature_7_to_28cm": "FLOAT",
             "soil_temperature_28_to_100cm": "FLOAT",
             "soil_temperature_100_to_255cm": "FLOAT",
             "soil_moisture_0_to_7cm": "FLOAT",
             "soil_moisture_7_to_28cm": "FLOAT",
             "soil_moisture_28_to_100cm": "FLOAT",
             "soil_moisture_100_to_255cm": "FLOAT"})
