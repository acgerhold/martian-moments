# Project Objectives and Goals

Technical objectives and business goals for the Mars rover data pipeline system.

## 1. Event-Driven Data Ingestion Architecture

**Objective:**  
Build a scalable, automated ingestion system that can process NASA Mars rover data across multiple rovers and data types using event-driven workflows.

**Implementation:**
- Kafka-based message streaming for DAG coordination and event triggering
- Airflow DAGs triggered by Kafka messages for photos, manifests, and coordinate data
- MinIO S3 events automatically trigger downstream processing when files are uploaded
- Batch scheduling system that dynamically generates ingestion tasks based on data availability

**Current Status:** ✅ Implemented
- Photo ingestion DAG with Kafka trigger
- Manifest and coordinate ingestion DAGs
- MinIO → Snowflake loading pipeline
- Automated dbt transformation triggered by load completion

## 2. Multi-Source NASA Data Integration

**Objective:**  
Integrate and harmonize data from multiple NASA APIs and data sources into a unified data model.

**Data Sources:**
- **NASA Mars Rover Photos API** - Image metadata for all rovers (Curiosity, Perseverance, Spirit, Opportunity)
- **NASA MMGIS Traverse Data** - GeoJSON coordinate tracking for Perseverance rover
- **NASA Manifest API** - Mission summaries and Sol-level statistics per rover

**Current Status:** ✅ Photos API integration, 🔄 Traverse data (Perseverance only)
- Photos ingestion with Sol-based batching
- JSON data storage in MinIO with organized folder structure
- Snowflake Bronze layer for raw data storage

## 3. Scalable Data Warehouse Architecture

**Objective:**  
Implement a modern data warehouse using medalion architecture (Bronze/Silver/Gold) for analytics and future applications.

**Architecture:**
- **Bronze Layer** - Raw JSON data from NASA APIs stored in Snowflake
- **Silver Layer** - Cleaned, normalized, and joined data models using dbt
- **Gold Layer** - Aggregated analytics tables and summary views

**Current Status:** ✅ Bronze layer, 🔄 Silver/Gold transformations
- dbt models for staging and dimensional modeling
- Automated transformation DAG triggered by data loads
- Data quality tests and validation models

## 4. Automated Batch Processing and Scheduling

**Objective:**  
Intelligent batch processing that optimizes API calls and manages data ingestion efficiently across multiple rovers and time periods.

**Features:**
- Dynamic Sol range batching based on data availability
- Intelligent scheduling to avoid API rate limits
- Gap detection and backfill capabilities
- Parallel processing of multiple rovers and data types

**Current Status:** ✅ Implemented
- Configurable batch sizes and Sol ranges
- Kafka-based scheduling coordination
- Parallel DAG execution with proper dependencies

## 5. Comprehensive Data Quality and Monitoring

**Objective:**  
Ensure data integrity and provide operational visibility into pipeline performance.

**Features:**
- dbt data quality tests for all models
- Comprehensive logging throughout the pipeline
- Error handling and retry mechanisms
- Data validation and anomaly detection

**Current Status:** 🔄 In Progress
- Logger utility with structured logging
- dbt tests for data validation
- Error handling in ingestion modules
- Pipeline monitoring via Airflow UI

## 6. Future: Image Processing and Timelapse Generation

**Future Objective:**  
Extend the pipeline to download actual rover images and generate automated timelapse videos.

**Planned Features:**
- Image download and storage in MinIO
- Image quality filtering and preprocessing
- Camera-specific timelapse generation
- Video encoding and output management

**Current Status:** 📋 Planned
- Infrastructure ready for image storage
- Metadata pipeline provides foundation for image processing

---

*Technical objectives for the Mars rover data ingestion and processing pipeline.*