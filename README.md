## Table of Contents
+ [Project Overview](#proove)
+ [Orchestration Requirement](#orcreq)		
+ [Pipeline Architecture](#piparc)
+ [Curation Process for Coordinates](#curpro)
+ [Visualization Sample](#vissam)

## Project Overview <a name = "proove"></a>

This is an ongoing project that began in late January of 2025. The goal of this project is to define a system for compiling U.S. energy and weather data, for visualization and analysis. The main focus is pulling data from online sources, transforming it, and then sending it to a PostgreSQL database.

## Orchestration Requirement <a name = "orcreq"></a>

Due to the nature of the data sources, pipelines will need to extract data at varying intervals. Some pipelines will fetch data every six hours, while others will only fetch data once per month. In order to orchestrate these tasks accordingly, multiple directed acyclic graphs (DAGs) are used, via Apache Airflow. One DAG for each pipeline. Separating the pipelines was done to keep the project more organized, as data is fetched from a number of sources at different intervals, and multiple datasets are generated from many of these sources. Data flows to PostgreSQL after extracting, cleaning, restructuring, and transforming it. The data can then be queried from PostgreSQL for visualization and analysis.

## Pipeline Architecture <a name = "piparc"></a>
![PipelineArchitecture](https://github.com/user-attachments/assets/c2f5e4c0-cd6e-44a9-92b2-d0dca3a756e4)

## Curation Process for Coordinates <a name = "curpro"></a>

For saving storage, while still preserving data quality, I chose to hand-curate coordinates for the weather pipeline. I recognize that this approach has its limitations; nonetheless, I feel it is a practical method for approximation, given the project’s current constraints. I estimate that once the pipeline is fully implemented, 108,000 datapoints will be extracted daily.

In choosing coordinates, I referred to power plant and substation locations, as well as satellite, powerline, population, climate, precipitation, and temperature maps. I aimed to capture as representative a sample as possible for each state, by selecting three coordinates which I thought might best represent each state’s topography, climate, and infrastructure. I made an effort to capture coordinates both in rural, and densely populated areas. Nationwide, all of the curated coordinates fall directly on top of powerlines. I chose a fairly balanced blend between major and minor voltage lines, to more accurately represent the grid, and to assist with examining transmission and distribution losses. To browse through the coordinates yourself, visit the curatedCoordinates.csv file.

## Visualization Sample <a name = "vissam"></a>
![HourlyNetGenerationByEnergySource](https://github.com/user-attachments/assets/faea6447-a6e9-48b6-ab4d-0c27a510f207)
