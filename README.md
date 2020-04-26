# Data_Logix Pipeline

Please create a conda env from **data_logix_env.yml**
This is the initial repository for BLX.

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

| Command | Description |
| --- | --- |
| git status | List all new or modified files |
| git diff | Show file differences that haven't been staged |
