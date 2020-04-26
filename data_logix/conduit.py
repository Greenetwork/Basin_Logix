'''
this file is intended to be used as the main processing pipeline of the water data
the data will come in from various sources, geospatial reference will be an important
aspect of the data processing. The end of a conduit run should produce one data point
with (WGS84) geosptially referenced with the parameters needed for water compliance and
water consumption calculations
'''

# start with various public datasets
# filter to san joaquin county for testing purposes
# filer (zoom) to one single farm or vineyar for testing purpose

import os
import pandas as pd
import geopandas as gpd
import logging

current_dir = os.getcwd()
logging.warning(f"current dir is {current_dir}") #showing user current dir
os.chdir('C:\\Users\\Alfahham\\Downloads\\California_Water_Data\\dwr') #todo REMOVE THIS PART AND PULL FROM data import
changed_dir = os.getcwd()
logging.warning(f"your directory was changed to {changed_dir}")

# WATER DISTRICTS DATA
# this is a polygon of each district with other val
dwr_water_districts_json = gpd.read_file("dwr_water_districts_042520.geojson")

# CIMIS AG CLIMATE/WEATHER DATA
cimis_station_locations_df = pd.read_excel("dwr_cimis_stations_list_042520.xlsx") # this will be helpful when we need to pull using the ICIMS API
#will drop the rows that contain NA in the Lat or Long cols
cimis_station_locations_df = cimis_station_locations_df.dropna(subset=["Longitude", "Latitude"])
#georeferencing the df of the cimis stations
cimis_station_locations_gdf = gpd.GeoDataFrame(cimis_station_locations_df,
                                               geometry = gpd.points_from_xy(cimis_station_locations_df.Longitude,
                                                                             cimis_station_locations_df.Latitude))
#CROP DATA
crop_information = gpd.read_file("i15_crop_mapping_2016_shp/i15_Crop_Mapping_2016.shp")
crop_information.crs = {'init': 'epsg:3310'}
crop_information_wgs = crop_information.to_crs({'init': 'epsg:4326'})

#COUNTIES
def get_geom_from_county_name(selected_county:str):
    counties = gpd.read_file("C:/Users/Alfahham/Downloads/California_Water_Data/CA_Counties/CA_Counties_TIGER2016.shp")
    counties = counties.to_crs({'init': 'epsg:4326'})
    selected_county_geom = counties[counties["NAME"] == selected_county]
    return selected_county_geom

san_joaquin_geom = get_geom_from_county_name("San Joaquin")

# FILTERED DATASETS SPATIALLY JOINED BY COUNTY
crop_information_san_joaquin = gpd.sjoin(crop_information_wgs, san_joaquin_geom, op = 'within') #lets add
crop_information_san_joaquin = crop_information_san_joaquin.drop(columns=['STATEFP', 'COUNTYFP', 'COUNTYNS',
                                                                                'GEOID', 'NAME', 'NAMELSAD', 'LSAD',
                                                                                'CLASSFP', 'MTFCC', 'CSAFP', 'CBSAFP',
                                                                                'METDIVFP', 'FUNCSTAT'])
dwr_water_districts_san_joaquin = gpd.sjoin(dwr_water_districts_json, san_joaquin_geom, op = 'within')
dwr_water_districts_san_joaquin = dwr_water_districts_san_joaquin.drop(columns=['STATEFP', 'COUNTYFP', 'COUNTYNS',
                                                                                'GEOID', 'NAME', 'NAMELSAD', 'LSAD',
                                                                                'CLASSFP', 'MTFCC', 'CSAFP', 'CBSAFP',
                                                                                'METDIVFP', 'FUNCSTAT'])


def load_california_parcel_data():
    global apn_parcels
    apn_parcels = gpd.read_file("C:/Users/Alfahham/Downloads/California_Water_Data/Parcels/Parcels.shp")
    apn_parcels = apn_parcels.dropna(subset=["geometry"])
    apn_parcels.crs = {'init': 'epsg:2227'}
    apn_parcels = apn_parcels.to_crs({'init': 'epsg:4326'})
    logging.warning("parcel data loaded")
    return apn_parcels

def filter_datasets_by_parcel_id(parcel_id: int, parcel_df):
    parcel_of_choice = parcel_df[parcel_df["APN"] == int(parcel_id)]
    return parcel_of_choice

def populate_an_aware_parcel(single_parcel_df):
    single_parcel_df = single_parcel_df.reset_index()
    populated_parcel_district_information = gpd.sjoin(single_parcel_df, dwr_water_districts_json, how="left", op='intersects').drop(columns=['index', "index_right", ])
    populated_parcel_district_and_crop_information = gpd.sjoin(populated_parcel_district_information, crop_information_wgs, how="left", op='intersects')
    return populated_parcel_district_and_crop_information

def tell_me_more_about_my_parcel(parcel_of_choice):
    parcel_of_choice_df = pd.DataFrame(parcel_of_choice)
    for index, row in parcel_of_choice_df.iterrows():
        your_apn = row["APN"]
        your_acreage = row["Acres"]
        your_region = row["Region"]
        your_agency = row["AGENCYNAME"]
        your_crop = row["Crop2016"]
        owner_name = row["Source"]
        print(f"for Parcel with APN:" + f"<b>{your_apn}<b>" + "the records show that your {your_acreage} acres plot" + '\n'
              f"fis in DWR Agency {your_agency} in {your_region} with {your_acreage} acre {your_crop} crops " + '\n'
              f"under the ownership of {owner_name}")

if __name__ == "__main__":
    single_parcel_df = filter_datasets_by_parcel_id(parcel_id=17328002, parcel_df=load_california_parcel_data())
    parcel_of_choice = populate_an_aware_parcel(single_parcel_df)
    parcel_of_choice_df = pd.DataFrame(parcel_of_choice)
    tell_me_more_about_my_parcel(parcel_of_choice_df)





