import fiona
import geopandas as gpd
import pandas as pd
import json
'''
In an attempt to standardize my data I will be turning the input data to a geojson in order to allow
to store it on github
'''

#convert geojson py

INPUT_PARCEL_DATA = "C:/Users/Alfahham/Downloads/California_Water_Data/Parcels/Parcels.shp"
APN_PARCELS = gpd.read_file(INPUT_PARCEL_DATA)
APN_PARCELS = APN_PARCELS.dropna(subset=["geometry"])
APN_PARCELS.crs = {'init': 'epsg:2227'}
APN_PARCELS = APN_PARCELS.to_crs({'init': 'epsg:4326'})
APN_PARCELS.to_file('PARCELS.geojson', driver='GeoJSON')

COUNTIES_PATH = "C:/Users/Alfahham/Downloads/California_Water_Data/CA_Counties/CA_Counties_TIGER2016.shp"
COUNTIES = gpd.read_file(COUNTIES_PATH)
COUNTIES = COUNTIES.to_crs({'init': 'epsg:4326'})
COUNTIES.to_file('COUNTIES.geojson', driver='GeoJSON')

CROP_PATH = "C:/Users/Alfahham/Downloads/California_Water_Data/dwr/CA_Crop_Mapping_2016/i15_Crop_Mapping_2016.shp"
CROP = gpd.read_file(CROP_PATH)
CROP.crs = {'init': 'epsg:3310'}
CROP = CROP.to_crs({'init': 'epsg:4326'})
CROP.to_file('CROPS.geojson', driver='GeoJSON')

DWR_WATER_QUALITY_PATH = "C:/Users/Alfahham/Downloads/California_Water_Data/dwr_water_quality.json"
DWR_WATER_DISTRICTS = json.loads(DWR_WATER_QUALITY_PATH)
DWR_WATER_DISTRICTS.crs

counties = fiona.open("CA_Counties_TIGER2016.zip")
