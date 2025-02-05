## Table of Contents
+ [Project Overview](#proove)
+ [Orchestration Requirement](#orcreq)		
+ [Pipeline Architecture](#piparc)
+ [Visualization Sample](#vissam)

## Project Overview <a name = "proove"></a>

This is an ongoing project that began in late January of 2025. The goal of this project is to define a system for compiling U.S. energy and weather data, for visualization and analysis. The main focus is pulling data from online sources, transforming it, and then sending it to a PostgreSQL database.

## Orchestration Requirement <a name = "orcreq"></a>

Due to the nature of the data sources, pipelines will need to extract data at varying intervals. Some pipelines will fetch data every six hours, while others will only fetch data once per month. In order to orchestrate these tasks accordingly, multiple directed acyclic graphs (DAGs) are used, via Apache Airflow. One DAG for each pipeline. Separating the pipelines was done to keep the project more organized, as data is fetched from a number of sources at different intervals, and multiple datasets are generated from many of these sources. Data flows to PostgreSQL after extracting, cleaning, restructuring, and transforming it. The data can then be queried from PostgreSQL for visualization and analysis.

## Pipeline Architecture <a name = "piparc"></a>
![PipelineArchitecture](https://github.com/user-attachments/assets/c2f5e4c0-cd6e-44a9-92b2-d0dca3a756e4)

## Visualization Sample <a name = "vissam"></a>
![HourlyNetGenerationByEnergySource](https://github.com/user-attachments/assets/faea6447-a6e9-48b6-ab4d-0c27a510f207)
