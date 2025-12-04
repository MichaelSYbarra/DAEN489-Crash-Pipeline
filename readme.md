# **Crash Report Pipeline**
## Overview
This project serves as an academic proof of concept for a live updating crash risk analysis pipeline.
The project covers from data extraction through processing all the way to the end result of predicting crash severity with a machine learning model,
while maintaining observability and metrics through Prometheus and Grafana. This is all wrapped up in a user friendly Streamlit Interface.
1. **The Datasets**  
The datasets involved in the project are the Chicago Open Data Portal crash datasets. These can be accessed through the following link: https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if
2. **The Data Flow**  
The data is first accessed via the extractor, which pulls in data and stores it into a MinIO datalake. The pipeline is automated so starting an extract call will also initiate a transform job in RabbitMQ. This merges the extracted data and pushes a clean job to RabbitMQ. The cleaner pulls the merged data from MinIO and ultimately stores it in a gold table in duckdb adhering to idempotency rules.
3. **The Model**  
The data in the gold table can be accessed through the streamlit interface to perform inference on it through the machine learning model. The model predicts a binary value, a crash that is able to be driven off or a crash that requires towing and/or emergency services. This prediction of crash severity allows particular features to examined in slices to make actionable decisions on how each factor influences the danger of our roads.
4. **The Interface**  
The Streamlit interface allows users to manage data, initiate pipeline runs, and either upload data or use the existent gold table for inference and prediction tasks. The interface also contains rudimentary monitoring of the pipeline health.

## Component Level Breakdown
1. **Extractor**  
The extractor code, written in GoLang, handles interfacing with the public facing apis provided by the Chicago Open Data Portal. Each call requires a JSON payload detailing enrichment columns and date ranges and styles. This data is pulled in batches and aggregated into a bucket in the MinIO datalake. A corrID based on time is generated for each run.
2. **Transformer**  
The transformer is in charge of taking the raw data from MinIO and converging it on the crash_record_ID value. The enrichment datasets have different formats and certain data points need to be aggregated to one or multiple columns to have a single row per crash instance. This data is then placed back in the MinIO datalake in the form of a merged.csv. This is done with readable and easily interpretable python scripts.
3. **Cleaner**  
The cleaner is a set of python scripts which handle pulling the previously made merged.csv from MinIO and ensuring it meets certain data standards. This step also transform rows into formats more conducive to machine learning tasks. Finally, the data is upserted into a DuckDB database where it remains as the gold table.
4. **Streamlit**  
The streamlit interface serves as the main way to interact with the pipeline. The first two main usages is a visual method of making the JSON payloads and intitiating
pipeline runs starting with the extractor. The second is using the machine learning model for inference on either an uploaded data csv or a user selected portion of gold table rows.
5. **Docker Compose**  
Docker is the main system which the pipeline is built around. Each service is built in an individual container for obserivability and individual maintenance and updates. Docker compose also contains the information neccesary for the less custom services such as MinIO, RabbitMQ, Prometheus, and Grafana to be spun up.
6. **Monitoring**  
Each service exposes metrics which are routinely scraped by Prometheus. Multiple Grafana dashboards with the most relevant information are also hosted.

## Visual Examples
This section contains screenshots of particular pipeline sections to demonstrate their functionality.  
  
### The Following three screenshots show expected logs of the extractor, transformer, and cleaner services after a successful run of the full pipeline. More logs will be shown when monitoring these services individually but for the sake of space only the final lines which should remain at the end of the service's successful runtime are displayed.  

![screenshot](./readme-assets/extract_logs.jpg "Extractor Logs")  
Extractor Logs  

![screenshot](./readme-assets/transform_logs.jpg "Transformer Logs")  
Transformer Logs  

![screenshot](./readme-assets/cleaner_logs.jpg "Cleaner Logs")  
Cleaner Logs  
  
### The following two images show the Streamlit page for fetching data and the page for using the machine learning model respectively. The ML model page also shows a snippet of the gold table values.

![screenshot](./readme-assets/datafetch.jpg "Data Fetcher")  
Data Fetcher  

![screenshot](./readme-assets/model_page.jpg "Model")  
ML Model Page  
  

### The following is an example of one of the Grafana dashboards. This one in particular shows the size and number of rows in the gold DuckDB database.

![screenshot](./readme-assets/duckdb.jpg "DuckDB Grafana Dashboard")  
DuckDB Grafana Dashboard  

### The last image in this section shows the target list for the Prometheus metrics.

![screenshot](./readme-assets/prometheus_scrapers.jpg "Prometheus Scrape List")  
Prometheus Scrape List  

## Architecture
The overall project architecture has multiple services in individual Docker containers. Users interact with them via either a Streamlit or Grafana interface. Messages and pipeline progress are tracked through exposing metrics scraped by Prometheus. Communication between containers is managed through RabbitMQ.

![diagram](./readme-assets/Project.png "Project Architecture Diagram")  
Project Architecture Diagram  

## How To Run The Pipeline
1. **Cloning the Repository**  
Clone the repository via download or git tools.
2. **Create a .env**   
In the root directory of the project create a .env file. For this project the following must be defined, default values have been provided:
```  
* ===== **RabbitMQ** =====
* RABBIT_USER=guest
* RABBIT_PASS=guest
* RABBIT_PORT=5672
* RABBIT_UI=15672
* RABBITMQ_URL=amqp://{RABBIT_USER}:{RABBIT_PASS}@rabbitmq:${RABBIT_PORT}/
* EXTRACT_QUEUE=extract
* CLEAN_QUEUE=clean

* ===== **MinIO** =====
* MINIO_USER=admin
* MINIO_PASS=admin123
* MINIO_API_PORT=9000
* MINIO_CONSOLE_PORT=9001
* MINIO_ENDPOINT=minio:${MINIO_API_PORT}
* MINIO_SSL=false

* ==== **Socrata** ====
* SOCRATA_BASE=https://data.cityofchicago.org
* ==== **Data** ====
* RAW_BUCKET=raw-data
* TRANSFORM_QUEUE=transform
* XFORM_BUCKET=transform-data
* GOLD_PATH = /data/gold/gold.duckdb
```
3. **Check Folder Structure**  
Ensure that the folder path data/gold/gold.duckdb exists and matches the value in your .env file.  

4. **Running the Project**  
Open a terminal in root directory of the project. Ensure that the Docker Daemon is running. Input the following to launch the project and all of the services:  
```
docker compose up -d
```  

5. **Browser Interfaces**  
Following the default values, use or update the following to access the web interfaces provided by the project:

   * Streamlit (`http://localhost:8501`)
   * Grafana (`http://localhost:3000`)
   * Prometheus (`http://localhost:9090`)

## Individual Additions  
This project while assigned and instructor led had much room for student-driven improvements and additions. Areas in particular which were added include but are not limited to:  
* EDA Pages
* Isolated DuckDB Metrics Exporter  
Useful as a method of checking if prior services were performing correctly as compared to exposing the metrics via the cleaner.  
*  Default auto-fills  
Usage of the streamlit interface is streamlined by default values for the project auto-populating enrichment columns etc.  

## Lessons learned  
This project touched on a great many different parts of devloping a full machine learning pipeline. Singling out lessons or difficult points is in itself, difficult. Regardless, a few examples:
### Difficulties  
* Implementing HTTP mini servers for exposing metrics  
* Implementing Cross-Container interactions via Docker and RabbitMQ
### Lessons Learned  
* Familiarity with industry standard practices and tools  
* Ability to research and implement new packages, tools, and libraries without direct guidance  
### Room for Improvement
* More detailed metrics  
* Cleaning up the streamlit interface slight usage annoyances

## Azure Setup
The migration of the project to a remote virtual machine encountered few roadblocks past Azure allowing the creation of the machine. The azure virtual machine was created to have a similar enviroment to that of the local ubuntu virtual machine. The same Ubuntu version was used along with pulling the lastest version of Docker.  

### Ports
The following ports were opened to allow access to the same localhosted services:  
```
Streamlit:
8501:8501  

Grafana:
3000:3000  

Prometheus:
9090:9090  

MinIO:
9000:9001
```  
  
### Folder Structure
The folder structure of the remote setup remain identical to the local machine's folder structure for the project:  
```
├── cleaner
├── data
│   └── gold
├── duckdb_exporter
├── extractor
├── grafana_data
├── minio-data
│   ├── raw-data
│   └── transform-data
├── monitoring
├── prometheus_data
├── readme-assets
├── streamlit
│   ├── artifacts
│   ├── pages
│   └── utils
└── transformer
```  

### Azure Examples
The following screenshots display the pipeline successfully running two full pipeline backfill tasks on the Azure virtual machine. The first screenshot shows the data management page after the first run showing data in both the gold table and the MinIO datalake. The second screenshot shows the overall report page after the second run of the pipeline, now with a larger gold table and new timestamp / corrID.

![screenshot](./readme-assets/remote_data_management.jpg "Remote Data Management")  
Azure remote pipeline's data management paged accessed through a web browser.  

![screenshot](./readme-assets/report_page_after_second_run.jpg "Report Page")  
Pipeline report page after a second run  
  
The following screenshot shows the logs of the extractor service running on the remote virtual machine. The output shows no difference in how the local and remote virtual machines run the pipeline. The final output of the logs details the sending of a transform job to RabbitMQ, expected behavior with no errors.  

![screenshot](./readme-assets/comparing_remote_logs.jpg "Remote Extractor Logs")  
Extractor logs on the remote virtual machine.  