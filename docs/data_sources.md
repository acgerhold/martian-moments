# Mars Rover Data Sources

Documentation of NASA's Mars Rover image data sources and APIs used in this project.

## Mars Rover Photos API

NASA's Mars Rover Photos API provides programmatic access to image data collected by the Curiosity, Opportunity, Spirit, and Perseverance rovers on Mars.

### Base Endpoints

```
https://api.nasa.gov/mars-photos/api/v1/rovers/perseverance/photos
https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/photos
https://api.nasa.gov/mars-photos/api/v1/rovers/opportunity/photos
https://api.nasa.gov/mars-photos/api/v1/rovers/spirit/photos
```

Photos are organized by **sol** (Martian rotation or day), counting from each rover's landing date. Each rover maintains its own photo database that can be queried separately.

### Query Parameters

| Parameter    | Type       | Default | Description |
|--------------|------------|---------|-------------|
| `sol`        | integer    | none    | Martian sol (0 to mission maximum) |
| `camera`     | string     | all     | Camera abbreviation (see table below) |
| `page`       | integer    | 1       | Pagination (25 items per page) |
| `earth_date` | YYYY-MM-DD | none    | Earth date corresponding to Martian sol |
| `api_key`    | string     | DEMO_KEY| Your NASA API key |

### Camera Systems by Rover

| Camera | Full Name | Curiosity | Opportunity | Spirit | Perseverance |
|--------|-----------|:---------:|:-----------:|:------:|:------------:|
| **FHAZ** | Front Hazard Avoidance Camera | ✅ | ✅ | ✅ | ✅ |
| **RHAZ** | Rear Hazard Avoidance Camera | ✅ | ✅ | ✅ | ✅ |
| **NAVCAM** | Navigation Camera | ✅ | ✅ | ✅ | ✅ |
| **MAST** | Mast Camera | ✅ | ❌ | ❌ | ✅ |
| **PANCAM** | Panoramic Camera | ❌ | ✅ | ✅ | ❌ |
| **CHEMCAM** | Chemistry and Camera Complex | ✅ | ❌ | ❌ | ❌ |
| **MAHLI** | Mars Hand Lens Imager | ✅ | ❌ | ❌ | ❌ |
| **MARDI** | Mars Descent Imager | ✅ | ❌ | ❌ | ❌ |
| **MINITES** | Miniature Thermal Emission Spectrometer | ❌ | ✅ | ✅ | ❌ |
| **SUPERCAM** | SuperCam | ❌ | ❌ | ❌ | ✅ |
| **PIXL** | Planetary Instrument for X-ray Lithochemistry | ❌ | ❌ | ❌ | ✅ |
| **SHERLOC** | Scanning Habitable Environments with Raman & Luminescence for Organics and Chemicals | ❌ | ❌ | ❌ | ✅ |
| **WATSON** | Wide Angle Topographic Sensor for Operations and eNgineering | ❌ | ❌ | ❌ | ✅ |
| **SKYCAM** | Sky Camera (part of MEDA) | ❌ | ❌ | ❌ | ✅ |
| **LCAM** | CacheCam (in sample caching system) | ❌ | ❌ | ❌ | ✅ |

### Example Request

```bash
curl "https://api.nasa.gov/mars-photos/api/v1/rovers/perseverance/photos?sol=100&camera=MAST&api_key=YOUR_API_KEY"
```

## NASA Planetary Data System (PDS) Archive

> **Note**: For Spirit and Opportunity images, full-resolution images must be retrieved directly from the PDS archive.

### Base URLs

```
https://planetarydata.jpl.nasa.gov/img/data/mer/spirit/
https://planetarydata.jpl.nasa.gov/img/data/mer/opportunity/
```

The PDS organizes images by rover and camera type using the following folder naming conventions:

| Camera | Spirit Directory | Opportunity Directory |
|--------|------------------|----------------------|
| **NAVCAM** | `mer2no_0xxx` | `mer1no_0xxx` |
| **FHAZ** | `mer2lo_0xxx` | `mer1lo_0xxx` |
| **RHAZ** | `mer2ro_0xxx` | `mer1ro_0xxx` |
| **PANCAM** | `mer2po_0xxx` | `mer1po_0xxx` |
| **MI** | `mer2co_0xxx` | `mer1co_0xxx` |

The `xxx` in folder names represents sol numbers (e.g., `mer2no_0001` for Spirit NAVCAM images from sol 1).

## Mars Rover Location Data

Track rover positions over time using NASA's traverse data:

| Rover | Endpoint |
|-------|----------|
| **Perseverance** | `https://mars.nasa.gov/mmgis-maps/M20/Layers/json/M20_traverse.json` |
| **Curiosity** | `https://mars.nasa.gov/mmgis-maps/MSL/Layers/json/MSL_traverse.json` |

**Data Notes:**
- Perseverance: Location data organized by sol
- Curiosity: Location data requires interpolation based on sol sequences  
- Opportunity/Spirit: No readily available traverse JSON

---

*Documentation for Mars rover image processing and timelapse generation project.*