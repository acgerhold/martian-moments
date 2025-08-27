## **Mars Rover Photos**

*https://api.nasa.gov/mars-photos/api/v1/rovers/perseverance/photos*  
*https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/photos*  
*https://api.nasa.gov/mars-photos/api/v1/rovers/opportunity/photos*  
*https://api.nasa.gov/mars-photos/api/v1/rovers/spirit/photos*  

<p>
This API is designed to collect image data gathered by NASA's Curiosity, Opportunity, and Spirit rovers on Mars and make it more easily available to other developers, educators, and citizen scientists.

Each rover has its own set of photos stored in the database, which can be queried separately. There are several possible queries that can be made against the API. Photos are organized by the sol (Martian rotation or day) on which they were taken, counting up from the rover's landing date
</p>

**Query Parameters**
| Parameter | Type    | Default  | Description |
| --------  | ------- | -------- | ----------- |
| sol       | int     | none     | 	sol (ranges from 0 to max found in endpoint)           |
| camera    | string  | json     |  see below for abbreviates            |
| page      | int     | 1        | 	25 items per page returned          |
| earth_date| YYYY-MM-DD | none | corresponding date on earth for the given sol

**Camera Index**
| Abbreviation | Camera                                                                               | Curiosity | Opportunity | Spirit | Perseverance    |
| ------------ | ------------------------------------------------------------------------------------ | --------- | ----------- | ------ | --------------- |
| FHAZ         | Front Hazard Avoidance Camera                                                        | ✔         | ✔           | ✔      | ✔               |
| RHAZ         | Rear Hazard Avoidance Camera                                                         | ✔         | ✔           | ✔      | ✔               |
| MAST         | Mast Camera                                                                          | ✔         |             |        | ✔  |
| CHEMCAM      | Chemistry and Camera Complex                                                         | ✔         |             |        |                 |
| MAHLI        | Mars Hand Lens Imager                                                                | ✔         |             |        |                 |
| MARDI        | Mars Descent Imager                                                                  | ✔         |             |        |                 |
| NAVCAM       | Navigation Camera                                                                    | ✔         | ✔           | ✔      | ✔               |
| PANCAM       | Panoramic Camera                                                                     |           | ✔           | ✔      |                 |
| MINITES      | Miniature Thermal Emission Spectrometer (Mini-TES)                                   |           | ✔           | ✔      |                 |
| SUPERCAM     | SuperCam                                                                             |           |             |        | ✔               |
| PIXL         | Planetary Instrument for X-ray Lithochemistry                                        |           |             |        | ✔               |
| SHERLOC      | Scanning Habitable Environments with Raman & Luminescence for Organics and Chemicals |           |             |        | ✔               |
| WATSON       | Wide Angle Topographic Sensor for Operations and eNgineering                         |           |             |        | ✔               |
| SKYCAM       | Sky Camera (part of MEDA)                                                            |           |             |        | ✔               |
| LCAM         | CacheCam (in sample caching system)                                                  |           |             |        | ✔               |


#### ! For Spirit and Opportunity images, must retrieve from Planetary Data System archive: 



## **NASA Planetary Data System Image Archive**

*https://planetarydata.jpl.nasa.gov/img/data/mer/spirit/*  
*https://planetarydata.jpl.nasa.gov/img/data/mer/opportunity/*

<p>
Publicly accessible archive that stores scientific data collected from NASA’s planetary missions. Specifically, the data includes camera images organized by rover, camera type, and Martian sol (solar day). These images are hosted by the Jet Propulsion Laboratory (JPL), which acts as a data node within the PDS. 

Image files are available in standard formats (e.g., JPEG, IMG) and structured in predictable directories, making them suitable for automated extraction, processing, and visualization tasks.

Image file names returned from Mars Rover Photos API match those found in PDS directories for each camera:
</p>

**Images Directories**
| Camera | Spirit Folder | Opportunity Folder |
| ------ | ------------- | ------------------ |
| NAVCAM | `mer2no_0xxx` | `mer1no_0xxx`      |
| FHAZ   | `mer2lo_0xxx` | `mer1lo_0xxx`      |
| RHAZ   | `mer2ro_0xxx` | `mer1ro_0xxx`      |
| PANCAM | `mer2po_0xxx` | `mer1po_0xxx`      |
| MI     | `mer2co_0xxx` | `mer1co_0xxx`      |

## **NASA Mars Rover Location Map**

*https://mars.nasa.gov/mmgis-maps/M20/Layers/json/M20_traverse.json* Perseverance  
*https://mars.nasa.gov/mmgis-maps/MSL/Layers/json/MSL_traverse.json* Curiosity

#### ! For Curiosity, location data is not organized by sol, will need to use sequence interpolation with Mars Rover Photos API
##### Maybe base this off of the speed/distance Perseverance is moving per sol