# Event Driven E-Commerce Data Platform
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://www.python.org/) 
[![Docker](https://img.shields.io/badge/Docker-Container-blue?logo=docker)](https://www.docker.com/) 
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-orange?logo=apacheairflow)](https://airflow.apache.org/) 
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql)](https://www.postgresql.org/) 
[![Kafka](https://img.shields.io/badge/Kafka-Streaming-red?logo=apachekafka)](https://kafka.apache.org/) 
[![PySpark](https://img.shields.io/badge/PySpark-ETL-orange?logo=apache-spark)](https://spark.apache.org/)

End-to-end real-time data engineering pipeline for e-commerce events, showcasing a Medallion architecture (Bronze → Silver → Gold), built with FastAPI, Kafka, NiFi, PostgreSQL, PySpark, Airflow, and containerized using Docker.
# 🚀 Architecture Overview
![Data System Design](docs/data_system_design.png)

FastAPI: Simulates order events

Kafka: 3-broker cluster

NiFi: Streams events into PostgreSQL

PostgreSQL: Stores raw & transformed data

PySpark: Medallion transformations

Airflow: DAG orchestration
