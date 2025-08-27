# martian-moments

Automated data pipeline transforming NASA Mars rover images into timelapse videos. Organizes photos by sol, camera, and mission for YouTube publishing with analytics dashboard.

## 🚀 Overview

Pulls images from NASA's Mars rover missions (Curiosity, Perseverance, Spirit, Opportunity), processes them into organized datasets, and generates automated timelapse videos for YouTube. Includes analytics dashboard for mission data insights and handles complex data harmonization across multiple NASA data sources.

## 🛠️ Tech Stack

**Data Pipeline:**
- Python - Core processing and automation
- Apache Airflow - Workflow orchestration and scheduling
- Snowflake - Data warehouse and analytics database
- MinIO - S3-compatible object storage for images and videos

**Image & Video Processing:**
- Pillow - Image format conversion and basic operations
- NumPy - Numerical computing and array operations
- MoviePy - Video generation and timelapse creation
- FuzzyWuzzy - Filename matching between NASA APIs and PDS archives

**Analytics & Visualization:**
- Pandas - Data manipulation and analysis
- CubeJS - Analytics API and data modeling
- Metabase - Business intelligence and dashboards

**Infrastructure:**
- Docker - Containerization and deployment

*Tech stack is iterative and subject to change as the project evolves.*

## 🗂️ Project Documentation

### [📊 Data Sources](docs/data_sources.md)
Documentation of NASA's Mars Rover APIs and image archives.

### [🎯 Project Objectives](docs/project_objectives.md)  
Key problems being solved in the data pipeline.

### [🏗️ Pipeline Architecture](docs/pipeline_architecture.md)
System design and data pipeline overview.