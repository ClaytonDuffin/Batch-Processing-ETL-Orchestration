## Table of Contents
+ [Project Overview](#proove)
+ [Getting Started](#getsta)
+ [Existing Pipelines](#exipip)
+ [Orchestration Requirement](#orcreq)
+ [Pipeline Architecture](#piparc)
+ [Curation Process for Coordinates](#curpro)
    + [Interactive Map](https://rawcdn.githack.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/a25225658ecbb9f0b749a2daf3422f3fb0ae3242/interactiveMapCuratedCoordinates.html)
+ [Visualization Sample](#vissam)

## Project Overview <a name = "proove"></a>

This project defines a system for compiling U.S. energy and weather data, for visualization and analysis. The main focus is extracting data from online sources, cleaning and transforming it, and then loading it into a PostgreSQL database.

## Getting Started <a name = "getsta"></a>

1. [Register for a free API key](https://www.eia.gov/opendata/register.php) from the U.S. Energy Information Administration (EIA). No API key is required to access the weather data from Open-Meteo.

2. [Download and install Docker Desktop](https://docs.docker.com/desktop/?_gl=1*1259ys3*_gcl_au*MTY5MTg5ODA4NS4xNzM5ODYyOTQz*_ga*OTQyOTY1NzAzLjE3Mzk4NTE5Nzk.*_ga_XJWPQMJYHQ*MTc0MTEwMzgzNC43LjEuMTc0MTEwMzkzNi40OS4wLjA.).

3. Next, clone this repository, and navigate into the cloned directory with the following command:
```
git clone https://github.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration.git && cd Batch-Processing-ETL-Orchestration
```

4. For a cloud deployment, set environment variables directly by running this command, after first inputting your own API key:
```
export AIRFLOW_UID=501 EIA_API_KEY="Enter Your API Key Here."
```

&emsp;&emsp;For a local deployment, create a `.env` file inside the local directory, and in it, add and save these two lines, inputting your own API key:
```
AIRFLOW_UID=501
EIA_API_KEY="Enter Your API Key Here."
```

5. Add a new folder titled `dags` to the cloned directory. Place `EIA7APipelineQuarterlyData.py`, `EIA814PipelineMonthlyData.py`, `EIA930PipelineHourlyData.py`, and `OpenMeteoWeatherPipelineHourlyData.py` inside this new folder. You can do this by running the following command:
```
mkdir -p dags && mv EIA7APipelineQuarterlyData.py EIA814PipelineMonthlyData.py EIA930PipelineHourlyData.py OpenMeteoWeatherPipelineHourlyData.py dags/
``` 

6. For this next step, if you are planning to make a cloud deployment, you must first update the connection details in `databaseOperations.py` accordingly. Once finished, or if you are planning to make a local deployment, run this command to build the application:
```
docker compose build
```

7. Once the application is built, you can deploy it locally or in a cloud environment by running this command:
```
docker compose up
```

8. Lastly, to stop the running containers and remove associated resources when finished, run the following command:
```
docker compose down
```

## Existing Pipelines <a name = "exipip"></a>

Data is curently sourced from two providers. [The U.S. Energy Information Administration (EIA)](https://www.eia.gov/opendata/), and [Open-Meteo](https://open-meteo.com/en/docs/historical-weather-api).

Listed below are the pipelines, with descriptions and properties, categorized by provider.

**U.S. Energy Information Administration (EIA)**

`EIA-7A Pipeline`
  * Extracts, transforms, and loads EIA-7A and MSHA 7000-2 form data (quarterly data).
  * Scheduled to run once per quarter, at midnight, on January 15th, April 15th, July 15th, and October 15th.
  * Data lags by 2 quarters.
  * Extracted data includes coal imports, exports, and shipment receipts.

`EIA-814 Pipeline`
  * Extracts, transforms, and loads EIA-814 form data (monthly data). 
  * Scheduled to run once per month, on the 15th, at midnight.
  * Data lags by 3 months.
  * Extracted data includes crude oil imports.

`EIA-930 Pipeline`
  * Extracts, transforms, and loads EIA-930 form data (hourly data).
  * Scheduled to run at 1 a.m. daily.
  * Data lags by 3 calendar days.
  * Extracted data includes electricity demand, day-ahead demand forecast, net generation, and interchange, by balancing authority.

**Open-Meteo**

`Historical Weather Pipeline`
  * Extracts, transforms, and loads Open-Meteo weather data (hourly data).
  * Scheduled to run at 1 a.m. daily. 
  * Data lags by 7 calendar days.
  * Extracted data includes 30 weather variables, at each of the 150 curated coordinates. 

## Orchestration Requirement <a name = "orcreq"></a>

Due to the nature of the data sources, the pipelines extract data at varying intervals. Some pipelines extract data daily, while others only extract data once per month. In order to orchestrate these tasks accordingly, multiple directed acyclic graphs (DAGs) are used, via Apache Airflow. One DAG for each pipeline. Data flows to PostgreSQL after extracting, cleaning, restructuring, and transforming it. The data can then be queried from PostgreSQL for visualization and analysis.

![AirflowUI](https://github.com/user-attachments/assets/44005c02-ca52-403b-928c-8d43808eb752)

## Pipeline Architecture <a name = "piparc"></a>
![PipelineArchitecture](https://github.com/user-attachments/assets/c2f5e4c0-cd6e-44a9-92b2-d0dca3a756e4)

## Curation Process for Coordinates <a name = "curpro"></a>

For saving storage, while still preserving data quality, I chose to hand-curate coordinates for the weather pipeline. I recognize that this approach has its limitations; nonetheless, I feel it is a practical method for approximation, given the project’s current constraints. As is, the weather pipeline extracts 108,000 datapoints daily.

In choosing coordinates, I referred to power plant and substation locations, as well as satellite, powerline, population, climate, precipitation, and temperature maps. I aimed to capture as representative a sample as possible for each state, by selecting three coordinates which I thought might best represent each state’s topography, climate, and infrastructure. I made an effort to capture coordinates both in rural, and densely populated areas. Nationwide, all of the curated coordinates fall directly on top of powerlines. I chose a fairly balanced blend between major and minor voltage lines, to more accurately represent the grid, and to assist with examining transmission and distribution losses. To explore the interactive map of curated coordinates on your own, [click here](https://rawcdn.githack.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/a25225658ecbb9f0b749a2daf3422f3fb0ae3242/interactiveMapCuratedCoordinates.html).

![zoomedOutMap](https://github.com/user-attachments/assets/40ea1617-c528-4acc-ac98-5070a7f140f6)
![zoomedInMap](https://github.com/user-attachments/assets/24eafd63-a3a7-45d3-82fa-a4d0d752978f)

## Visualization Sample <a name = "vissam"></a>
![HourlyNetGenerationByEnergySource](https://github.com/user-attachments/assets/faea6447-a6e9-48b6-ab4d-0c27a510f207)
