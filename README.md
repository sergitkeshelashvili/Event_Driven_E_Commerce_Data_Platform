# Event Driven E-Commerce Data Platform

End-to-end real-time data engineering pipeline for e-commerce events, showcasing a Medallion architecture (Bronze → Silver → Gold), built with FastAPI, Kafka, NiFi, PostgreSQL, PySpark, Airflow, and containerized using Docker.
# 🚀 Architecture Overview
![Data System Design](docs/data_system_design.png)

FastAPI: Simulates order events

Kafka: 3-broker cluster

NiFi: Streams events into PostgreSQL

PostgreSQL: Stores raw & transformed data

PySpark: Medallion transformations

Airflow: DAG orchestration
