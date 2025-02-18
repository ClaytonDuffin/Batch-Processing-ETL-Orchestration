## Table of Contents
+ [Project Overview](#proove)
+ [Orchestration Requirement](#orcreq)		
+ [Pipeline Architecture](#piparc)
+ [Curation Process for Coordinates](#curpro)
    + [Interactive Map](https://rawcdn.githack.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/a25225658ecbb9f0b749a2daf3422f3fb0ae3242/interactiveMapCuratedCoordinates.html)
+ [Visualization Sample](#vissam)

## Project Overview <a name = "proove"></a>

This is an ongoing project that began in late January of 2025. The goal of this project is to define a system for compiling U.S. energy and weather data, for visualization and analysis. The main focus is pulling data from online sources, transforming it, and then sending it to a PostgreSQL database.

## Orchestration Requirement <a name = "orcreq"></a>

Due to the nature of the data sources, the pipelines extract data at varying intervals. Some pipelines extract data daily, while others only extract data once per month. In order to orchestrate these tasks accordingly, multiple directed acyclic graphs (DAGs) are used, via Apache Airflow. One DAG for each pipeline. Data flows to PostgreSQL after extracting, cleaning, restructuring, and transforming it. The data can then be queried from PostgreSQL for visualization and analysis.

## Pipeline Architecture <a name = "piparc"></a>
![PipelineArchitecture](https://github.com/user-attachments/assets/c2f5e4c0-cd6e-44a9-92b2-d0dca3a756e4)

## Curation Process for Coordinates <a name = "curpro"></a>

For saving storage, while still preserving data quality, I chose to hand-curate coordinates for the weather pipeline. I recognize that this approach has its limitations; nonetheless, I feel it is a practical method for approximation, given the project’s current constraints. As is, the weather pipeline extracts 108,000 datapoints daily.

In choosing coordinates, I referred to power plant and substation locations, as well as satellite, powerline, population, climate, precipitation, and temperature maps. I aimed to capture as representative a sample as possible for each state, by selecting three coordinates which I thought might best represent each state’s topography, climate, and infrastructure. I made an effort to capture coordinates both in rural, and densely populated areas. Nationwide, all of the curated coordinates fall directly on top of powerlines. I chose a fairly balanced blend between major and minor voltage lines, to more accurately represent the grid, and to assist with examining transmission and distribution losses. To explore the interactive map of curated coordinates on your own, [click here](https://rawcdn.githack.com/ClaytonDuffin/Batch-Processing-ETL-Orchestration/a25225658ecbb9f0b749a2daf3422f3fb0ae3242/interactiveMapCuratedCoordinates.html).

![zoomedOutMap](https://github.com/user-attachments/assets/40ea1617-c528-4acc-ac98-5070a7f140f6)
![zoomedInMap](https://github.com/user-attachments/assets/24eafd63-a3a7-45d3-82fa-a4d0d752978f)

## Visualization Sample <a name = "vissam"></a>
![HourlyNetGenerationByEnergySource](https://github.com/user-attachments/assets/faea6447-a6e9-48b6-ab4d-0c27a510f207)
