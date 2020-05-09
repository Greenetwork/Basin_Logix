# Data_Logix Pipeline

Please create a conda env from **data_logix_env.yml**
This is the initial repository for BLX.

**Marco Tasks:**

- [ ] **conduit.py**: generalized pipeline that can "query" important water district information about an APN number from various datasets

*[ ]* **conduit.py**: conduit should only be limited to the APN > Relevant Input Params layer

*[ ]* **conduit.py**: modularize in order to allow outputs based on user provided APN

*[ ]* **conduit.py**: outputs should comply with NFT input parameters

[ ] 

Data Sources:
- California Department of Water Resources (water rights?)
- USDA (land/crop categorization)
- NOAA (Temp, preciptiation, drought conditions)

Data Structures:
- csv
- GeoJSON
- shp
- Tiff

| Global Parameter  | Data Type | Description |
| ------------- | ------------- | -------------| 
| apn_number  | uint  | Show file differences that haven't been staged |
| apn_location  | geom  | Show file differences that haven't been staged |
| dwr_bulletin  | str  | Show file differences that haven't been staged |
| gsa_name  | str  | Show file differences that haven't been staged |
| wtr_rights_type  | str  | Show file differences that haven't been staged |
| wtr_rights_date_issued  | datetime  | Show file differences that haven't been staged |
| wtr_rights_scan  | Unk  | Show file differences that haven't been staged |
| parcel_acerage  | int  | Show file differences that haven't been staged |
| parcel_area_meter_sq  | geom  | | Show file differences that haven't been staged |

data_logix/conduit_input_data
| file name | Description |
| --- | --- |
| COUNTIES.geojson | List all new or modified files |
| CROPS.geojson | Show file differences that haven't been staged |
| PARCELS.geojson | List all new or modified files |
| WATER_DISTRICTS.geojson | Show file differences that haven't been staged |


