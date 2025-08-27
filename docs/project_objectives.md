# Project Problems and Goals

Technical challenges and objectives for the Mars rover timelapse data pipeline.

## 1. Image Replacement & Normalization (Rover API → PDS Archive)

**Problem:**  
NASA's Mars Rover API returns image metadata for all rovers, but image URLs for Spirit and Opportunity are defunct or redirect due to broken links.

**Goal:**  
Replace broken image links with valid equivalents from the PDS Imaging Archive using filename matching.

- Extract metadata from the API and map filenames to PDS directory structures
- Add lookup step in pipeline to attach correct image URLs based on filename and camera
- Run ingestion once for inactive rovers (Spirit and Opportunity)

## 2. Temporal Normalization of Rover Path Data (Curiosity)

**Problem:**  
Curiosity's traverse data is not organized by Sol numbers or Earth dates like Perseverance, making synchronization with photo and weather data difficult.

**Goal:**  
Reconstruct rover path as a daily (sol-level) timeline by interpolating coordinate data and approximating Sols.

- Estimate Sol by dividing total distance into average meters per Sol
- Synchronize coordinate data with image metadata via estimated Sol
- Use Perseverance movement patterns to validate assumptions
- Build consistent Sol-indexed path data from raw coordinate points

## 3. Image Quality & Type Standardization for Video Production

**Problem:**  
Raw rover images vary significantly in resolution, color processing, file format, and quality across different cameras and missions.

**Goal:**  
Implement image preprocessing and filtering to create standardized, high-quality video frames.

- **Color Correction:** Normalize white balance and exposure across different lighting conditions and camera sensors
- **Resolution Standardization:** Resize/crop images to consistent dimensions while preserving aspect ratios  
- **Quality Filtering:** Automatically detect and exclude corrupted, overexposed, or heavily artifacted images
- **Format Conversion:** Convert various source formats (JPEG, PNG, IMG) to consistent output format
- **Temporal Consistency:** Apply consistent processing parameters across sequences to avoid flickering
- **Camera-Specific Profiles:** Create preprocessing pipelines tailored to each camera's characteristics

## 4. Multi-Source Data Harmonization (API + PDS + MMGIS)

**Problem:**  
Data from different missions is inconsistent in format and availability across multiple data sources.

**Goal:**  
Create unified schema and data model for handling rovers, positions, images, and metadata.

- Define consistent fields: sol, earth_date, rover_name, camera, coordinates, image_url, processing_metadata
- Write custom extractors for each data source (API, directory crawler, JSON traverse)
- Detect and resolve mismatches or missing values (image links, missing coordinates)
- Track image processing decisions for analytics and quality control

## 5. Time-Series Construction for Timelapse Visualization

**Problem:**  
Raw image sequences are unordered, unequally spaced, and contain gaps that make smooth timelapse creation challenging.

**Goal:**  
Build frame-ready image sequences by organizing images by camera and Sol, filtering incomplete sets.

- Select specific cameras and create filtered lists of quality images
- Interpolate missing days or integrate rover path data for spatial context
- Export frame sequences optimized for video encoding
- Handle temporal gaps and maintain consistent frame rates across different mission periods

---

*Technical objectives for automated Mars rover image processing and timelapse generation.*