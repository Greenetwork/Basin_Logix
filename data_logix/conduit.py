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

#conduit py

import os
import pandas as pd
import geopandas as gpd
import logging
import fiona

def check_for_conduit_input_data():
    current_dir = os.getcwd()
    if "Basin_Logix" in current_dir.split("\\"):
            logging.warning("Basin_Logix directory detected - all input files will be referenced from this directory")
        else:
            logging.warning(" PLEASE CHANGE YOUR DIRECTORY TO BASIN LOGIX REPO - "
                            " for input files to work your current dir needs to be ~Basin_Logix") #showing user current dir

def check_for_conduit_input_files():
    current_dir = os.getcwd()
    conduit_input_file_path = "\data_logix\conduit_input_data"
    full_path = os.path.join(current_dir + conduit_input_file_path)
    logging.warning(f"checking for input files in {full_path}")
    list_of_files_in_data_path = os.listdir(full_path)
    if "WATER_DISTRICTS.geojson" in list_of_files_in_data_path:
        print("water districts input file detected")
        if "PARCELS.geojson" in list_of_files_in_data_path:
            print("parcels input file detected")
            if "CROPS.geojson" in list_of_files_in_data_path:
                print("crops input file detected")
                if "COUNTIES.geojson" in list_of_files_in_data_path:
                    print("counties input file detected")
    else:
        logging.warning("MISSING INPUT FILES")

def load_water_districts_data():
    water_districts = gpd.read_file("data_logix/conduit_input_data/WATER_DISTRICTS.geojson")
    assert water_districts.crs == {'init': 'epsg:4326'}
    assert type(water_districts.geometry) == (gpd.geoseries.GeoSeries)
    logging.warning("water districts data loaded")
    return water_districts

def load_california_parcel_data():
    apn_parcels = gpd.read_file("data_logix/conduit_input_data/PARCELS.geojson")
    assert apn_parcels.crs == {'init': 'epsg:4326'} # making sure input data file is in the right crs
    assert type(apn_parcels.geometry) == (gpd.geoseries.GeoSeries) #making sure i have a geom as geoseries
    logging.warning("parcel data loaded")
    return apn_parcels

def load_california_crops_data():
    crops = gpd.read_file("data_logix/conduit_input_data/CROPS.geojson")
    assert crops.crs == {'init': 'epsg:4326'} # making sure input data file is in the right crs
    assert type(crops.geometry) == (gpd.geoseries.GeoSeries) #making sure i have a geom as geoseries
    logging.warning("crop data loaded")
    return crops

def load_california_counties_data():
    counties = gpd.read_file("data_logix/conduit_input_data/COUNTIES.geojson")
    assert counties.crs == {'init': 'epsg:4326'} # making sure input data file is in the right crs
    assert type(counties.geometry) == (gpd.geoseries.GeoSeries) #making sure i have a geom as geoseries
    logging.warning("counties data loaded")
    return counties

def get_geom_from_county_name(counties_data_frame, selected_county:str):
    selected_county_geom = counties_data_frame[counties_data_frame["NAME"] == selected_county]
    return selected_county_geom

def filter_data_by_county(data_frame_to_filter, county_geom):
crop_information_san_joaquin = gpd.sjoin(data_frame_to_filter, county_geom, op='within')  # lets add
data_frame_to_filter = data_frame_to_filter.drop(columns=['STATEFP', 'COUNTYFP', 'COUNTYNS',
                                                                                'GEOID', 'NAME', 'NAMELSAD', 'LSAD',
                                                                                'CLASSFP', 'MTFCC', 'CSAFP', 'CBSAFP',
                                                                                'METDIVFP', 'FUNCSTAT'])

dwr_water_districts_san_joaquin = dwr_water_districts_san_joaquin.drop(columns=['STATEFP', 'COUNTYFP', 'COUNTYNS',
                                                                                'GEOID', 'NAME', 'NAMELSAD', 'LSAD',
                                                                                'CLASSFP', 'MTFCC', 'CSAFP', 'CBSAFP',
                                                                                'METDIVFP', 'FUNCSTAT'])

def filter_datasets_by_parcel_id(parcel_id: int, parcel_df):
    parcel_of_choice = parcel_df[parcel_df["APN"] == int(parcel_id)]
    return parcel_of_choice

def populate_an_aware_parcel(single_parcel_df, water_districts_df):
    single_parcel_df = single_parcel_df.reset_index()
    populated_parcel_district_information = gpd.sjoin(single_parcel_df, water_districts_df, how="left", op='intersects').drop(columns=['index', "index_right", ])
    populated_parcel_district_and_crop_information = gpd.sjoin(populated_parcel_district_information, water_districts_df, how="left", op='intersects')
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
    check_for_conduit_input_data()
    check_for_conduit_input_files()
    COUNTIES = load_california_counties_data()
    san_joaquin_geom = get_geom_from_county_name(counties_data_frame=COUNTIES, selected_county="San Joaquin") #nice source of geom for the county but i dont use this somewhere else


    single_parcel_df = filter_datasets_by_parcel_id(parcel_id=17328002, parcel_df=load_california_parcel_data())
    parcel_of_choice = populate_an_aware_parcel(single_parcel_df, water_districts_df=load_water_districts_data())
    parcel_of_choice_df = pd.DataFrame(parcel_of_choice)
    tell_me_more_about_my_parcel(parcel_of_choice_df)


