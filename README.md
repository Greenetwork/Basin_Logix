# Basin_Logix

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
| apn_number  | uint  | Content Cell |
| apn_location  | geom  | Content Cell |
| dwr_bulletin  | str  | Content Cell |
| gsa_name  | str  | Content Cell |
| wtr_rights_type  | str  | Content Cell |
| wtr_rights_date_issued  | datetime  | Content Cell |
| wtr_rights_scan  | Unk  | Content Cell |
| parcel_acerage  | int  | Content Cell |
| parcel_area_meter_sq  | geom  | Content Cell |
