# External Links Analysis Pipeline

## Project Overview
This pipeline extracts external links from Common Crawl data and stores the results in a PostgreSQL database, with enriched metadata including country of origin, content type, and a flag for ad-based domains. 
The final processed data is stored in a columnar format for easy analysis.
The pipeline is orchestrated by Apache Airflow, that is triggered by the arrival of a new file.

## Flow of the Pipeline
1. Extract Links: Parse and extract all external links from Common Crawl files. 
2. Load Data to PostgreSQL: Store extracted links in a PostgreSQL database (running on Docker)
    - Table **web_archive_db.web_crawling_urls**
3. Enrich Data: Add country, content type, and ad-based flags using external APIs (using the **Spark Cluster**). 
    - Table **/airflow/dags/data/silver_web_urls**
4. Save Data: Save the final dataset in columnar (Parquet) format, partitioned by country. 
    - Table **/airflow/dags/data/gold_urls_categorized**
5. Compute Metrics: Calculate useful metrics on domains, subsections, countries, etc. (using a **Jupyter Notebook** for visualization)

## Prerequisites
- Docker and Docker Compose


## Quickstart

### 1. Download Common Crawl Data
```bash
airflow/dags/data/commoncrawl_download.sh <crawl> <segment_1>,<segment_2>
```
For example:
```bash
airflow/dags/data/commoncrawl_download.sh CC-MAIN-2024-38 1725700650826.4
```

### 2. Enviroment setup
To build docker images:
```makefile
make build
```
To create containers:
```makefile
make up
```

## Architecture Overview

This pipeline follows a modular architecture that leverages Docker to deploy components essential for link extraction, transformation, enrichment, and storage. The main components include an Apache Airflow orchestration setup, a PostgreSQL database, a distributed Spark cluster for scalable data processing, and a Jupyter notebook for analysis and visualization.

### Airflow
Apache Airflow is used to orchestrate and automate the pipeline’s end-to-end data workflow.

1. **Webserver**: The Airflow Webserver hosts the web interface for managing and monitoring workflows.

2. **Scheduler**: The Airflow Scheduler is responsible for scheduling and executing tasks defined in the Airflow DAGs.

3. **Celery Worker 1** and **Celery Worker 2**: Celery Workers are responsible for running the tasks in parallel, allowing the pipeline to scale horizontally.

4. **Redis**: Redis serves as the message broker for Airflow's Celery workers.

### PostgreSQL Database
The PostgreSQL database serves as the central storage for extracted links. It

### Spark Cluster
Apache Spark is the core processing engine used for extracting and transforming data.

1. **Spark Master**: The Spark Master node coordinates data processing tasks across the Spark Workers.

2. **Spark Worker 1** and **Spark Worker 2**: Each Spark Worker processes chunks of data in parallel, contributing to faster data extraction, enrichment, and aggregation tasks.

### Jupyter
Jupyter is used for interactive data visualization of the metrics.