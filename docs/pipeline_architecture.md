# Pipeline Architecture

Event-driven data pipeline architecture for NASA Mars rover data ingestion, processing, and analytics.

## 🏗️ System Overview

The Martian Moments pipeline is built on event-driven architecture using Apache Kafka for message coordination and Apache Airflow for workflow orchestration. The system processes NASA Mars rover data through a medallion architecture (Bronze/Silver/Gold) with automated batch scheduling and real-time event processing.

![Martian Moments Pipeline Architecture](Martian%20Moments%20Diagram.jpg)

## 📊 Data Flow Architecture

### 1. Ingestion Layer (Bronze)

**Event-Driven Scheduling:**
- dbt transformation produces `INGESTION_PLANNING` view identifying data gaps
- Transformation DAG sends scheduling messages to Kafka `ingestion-scheduling` topic
- Multiple ingestion DAGs subscribe to scheduling events and process batches in parallel

**Data Sources & Processing:**
- **Photos API**: Batched Sol-range requests with parallel rover processing
- **Manifest API**: Mission metadata and Sol statistics per rover
- **Traverse Data**: GeoJSON coordinate tracking (currently Perseverance only)

**Output:** Raw JSON files stored in MinIO with organized folder structure

### 2. Loading Layer

**Automated File Processing:**
- MinIO S3 events automatically trigger Kafka `minio-events` topic on file uploads
- Snowflake Load DAG extracts JSONL data and loads to Bronze schema tables
- Load completion triggers Kafka `snowflake-load-complete` topic

**Data Organization:**
```
MinIO Bucket Structure:
├── photos/mars_rover_photos_batch_sol_X_to_Y_timestamp.json
├── coordinates/mars_rover_coordinates_batch_timestamp.json
└── manifests/mars_rover_manifests_batch_timestamp.json

Snowflake Bronze Schema:
├── RAW_PHOTO_RESPONSE
├── RAW_COORDINATE_RESPONSE
└── RAW_MANIFEST_RESPONSE
```

### 3. Transformation Layer (Silver/Gold)

**dbt-Powered Analytics:**
- Load completion events trigger dbt model execution
- Automated staging models flatten JSON into dimensional tables
- Gold layer aggregations provide analytics-ready datasets

**Data Models:**
```
Silver Layer (Staging):
├── dim_rovers
├── dim_cameras  
├── dim_coordinates
├── fact_photos
├── fact_path
└── ingestion_planning (gap detection)

Gold Layer (Marts):
├── photo_counts
├── rover_summary
├── camera_summary
├── daily_activity
└── photo_travel_correlation
```

## ⚙️ Infrastructure Components

### Container Architecture

**Docker Compose Services:**
- **Airflow** (Webserver, Scheduler, Workers): Workflow orchestration
- **PostgreSQL**: Airflow metadata database
- **Redis**: Celery task queue for distributed processing
- **Kafka**: Event streaming and message coordination
- **MinIO**: S3-compatible object storage with event notifications
- **Python Environment**: Custom image with all dependencies

### Event-Driven Coordination

**Kafka Topics:**
- `ingestion-scheduling`: Coordinates batch processing schedules
- `minio-events`: File upload notifications from MinIO
- `snowflake-load-complete`: Triggers downstream transformations

**Airflow Asset Triggers:**
- DAGs subscribe to Kafka topics using MessageQueueTrigger
- Asset-based dependencies ensure proper execution order
- Parallel processing with configurable concurrency limits

## 🔄 Workflow Orchestration

### DAG Architecture

**1. Transformation DAG** (`run_dbt_models`)
- **Trigger**: `snowflake-load-complete` Kafka topic
- **Purpose**: Run dbt models and generate ingestion schedules
- **Output**: Scheduling messages to Kafka

**2. Ingestion DAGs**
- **Photo Ingestion** (`mars_rover_photos_ingestion`)
- **Manifest Ingestion** (`mars_rover_manifest_ingestion`) 
- **Coordinate Ingestion** (`mars_rover_coordinates_ingestion`)
- **Trigger**: `ingestion-scheduling` Kafka topic
- **Processing**: Parallel API calls with batch optimization

**3. Loading DAG** (`load_to_snowflake`)
- **Trigger**: `minio-events` Kafka topic
- **Purpose**: Extract JSONL from MinIO and load to Snowflake
- **Output**: Load completion events

### Batch Processing Strategy

**Intelligent Scheduling:**
- dbt `ingestion_planning` view identifies missing Sol ranges per rover
- Dynamic batch generation based on data availability and API limits
- Configurable batch sizes (default: 10 Sol range per batch)
- Automatic gap detection and backfill coordination

**Parallel Execution:**
- Multiple rovers processed simultaneously
- Sol ranges within batches processed in parallel
- Independent DAGs for different data types
- Celery workers distribute tasks across containers

## 🛡️ Data Quality & Monitoring

### Quality Assurance

**dbt Testing Framework:**
- Data validation tests for all staging and mart models
- Relationship integrity tests between dimensions and facts
- Range validation for Sol values and coordinates
- Camera-rover combination validation

**Error Handling:**
- Comprehensive logging with structured format
- Graceful error handling with retry mechanisms
- Failed task isolation prevents pipeline blocking
- Data quality alerts and monitoring

### Operational Monitoring

**Airflow Monitoring:**
- Web UI for DAG execution monitoring
- Task logs and error tracking
- Performance metrics and execution history
- Alert notifications for failed tasks

**Data Lineage:**
- dbt documentation with full lineage graphs
- Asset dependencies tracked in Airflow
- Source-to-mart data flow visibility
- Impact analysis for schema changes

## 🚀 Scalability & Performance

### Horizontal Scaling

**Celery Worker Scaling:**
- Additional worker containers can be added
- Task distribution across multiple workers
- Independent scaling of different task types

**Database Optimization:**
- Snowflake auto-scaling for compute resources
- Partitioned tables for efficient querying
- Incremental dbt models for large datasets

### Performance Features

**Batch Optimization:**
- Configurable batch sizes based on API limits
- Parallel API requests within batches
- Efficient JSON processing and storage
- Minimal data movement between layers

**Caching Strategy:**
- MinIO object storage reduces API re-calls
- dbt incremental models for efficient transformations
- Redis caching for Celery task coordination

---

*Architecture documentation for the Mars rover data pipeline system.*
