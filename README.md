## Table of Contents
+ [Project Overview](#proove)
+ [Orchestration Requirement](#orcreq)		
+ [Pipeline Architecture](#piparc)
+ [Visualization Sample](#vissam)

## Project Overview <a name = "proove"></a>

This is an ongoing project that began in late January of 2025. The goal of this project is to define a system for compiling U.S. energy and weather data, for visualization and analysis. The main focus is pulling data from online sources, transforming it, and then sending it to a PostgreSQL database.

## Orchestration Requirement <a name = "orcreq"></a>

Due to the nature of the data sources, multiple pipelines will need to extract data at varying intervals. Some pipelines will fetch data every six hours, while others will only fetch data once per month. In order to orchestrate these tasks accordingly, multiple directed acyclic graphs (DAGs) are used, via Apache Airflow. One DAG for each pipeline. Separating the pipelines was done to keep the project more organized, as data is fetched from a number of sources at different intervals, and multiple datasets are generated from many of these sources. Data flows to PostgreSQL after extracting, cleaning, restructuring, and transforming it. The data can then be queried from PostgreSQL for visualization and analysis.

## Pipeline Architecture <a name = "piparc"></a>
Here is a graph which shows the general structure of each pipeline:
![PipelineArchitecture](https://github.com/user-attachments/assets/c2f5e4c0-cd6e-44a9-92b2-d0dca3a756e4)

## Visualization Sample <a name = "vissam"></a>
Here is a graph showing a sample of some of the data being extracted:
![HourlyNetGenerationByEnergySource](https://github.com/user-attachments/assets/ac98e737-9aad-4fc0-91c1-7e1d302c5577)
