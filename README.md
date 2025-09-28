# martian-moments

Event-driven data pipeline for ingesting, processing, and analyzing NASA Mars rover mission data. Collects photos, coordinate tracking, and mission manifests from multiple rovers using automated batch processing with Kafka-triggered workflows.

## 🚀 Overview

Automated ingestion system that pulls data from NASA's Mars rover missions (Curiosity, Perseverance, Spirit, Opportunity) and processes it through a multi-stage data pipeline. The system handles complex batch scheduling, real-time event processing, and data transformation for analytics and future timelapse video generation.

![Martian Moments Pipeline Architecture](docs/Martian%20Moments%20Diagram.jpg)

## 🛠️ Tech Stack

**Data Pipeline & Orchestration:**
- Apache Airflow - Workflow orchestration with event-driven DAGs
- Apache Kafka - Message streaming and event coordination
- Python - Core processing, API integrations, and data transformation
- Docker & Docker Compose - Containerized deployment

**Data Storage & Processing:**
- Snowflake - Data warehouse with Bronze/Silver/Gold layers
- MinIO - S3-compatible object storage for JSON data files
- dbt - Data transformation and modeling framework
- PostgreSQL - Airflow metadata database
- Redis - Celery task queue backend

**Data Integration:**
- NASA Mars Rover Photos API - Image metadata and mission data
- NASA MMGIS Traverse Data - Rover coordinate tracking (GeoJSON)
- Snowflake Connector - Direct data loading and SQL operations
- Pandas - Data manipulation and batch processing

**Infrastructure:**
- Confluent Kafka - Enterprise messaging platform
- MinIO S3 Events - Automated file upload notifications
- Celery - Distributed task execution
- pytest - Comprehensive testing framework

*Architecture is event-driven and designed for scalable, automated data processing workflows.*

## 🗂️ Project Documentation

### [📊 Data Sources](docs/data_sources.md)
Documentation of NASA's Mars Rover APIs and image archives.

### [🎯 Project Objectives](docs/project_objectives.md)  
Key problems being solved in the data pipeline.

### [🏗️ Pipeline Architecture](docs/pipeline_architecture.md)
System design and data pipeline overview.