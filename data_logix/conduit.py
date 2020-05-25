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
import sys
import pandas as pd
import geopandas as gpd
import logging
from pathlib import Path, PureWindowsPath, PurePosixPath
import fiona

def check_for_conduit_input_data():
    '''this function is just checking if your console is in the basin logix repo
    it doesnt check the input data sources yet'''
    current_dir = os.getcwd()
    if "Basin_Logix" == os.getcwd().split('/')[-1].split('\\')[-1]:
            logging.warning("Basin_Logix directory detected - all input files will be referenced from this directory")
    else:
        logging.warning(" PLEASE CHANGE YOUR DIRECTORY TO BASIN LOGIX REPO - "
                            " for input files to work your current dir needs to be ~Basin_Logix") #showing user current dir

def check_for_conduit_input_files():
    current_dir = os.getcwd()
    full_path = os.path.join(current_dir)
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

def load_ewrims_diversion_data():
    points_of_diversion = gpd.read_file("eWRIMS_POINTS_OF_DIVERSION.geojson")
    assert points_of_diversion.crs == {'init': 'epsg:4326'}
    assert type(points_of_diversion.geometry) == (gpd.geoseries.GeoSeries)
    logging.warning("eWRIMS POINTS OF DIVERSION - Loaded")
    return points_of_diversion

def load_water_districts_data():
    water_districts = gpd.read_file("WATER_DISTRICTS.geojson")
    assert water_districts.crs == {'init': 'epsg:4326'}
    assert type(water_districts.geometry) == (gpd.geoseries.GeoSeries)
    logging.warning("WATER DISTRICTS data - Loaded")
    return water_districts

def load_california_parcel_data():
    apn_parcels = gpd.read_file("PARCELS.geojson")
    assert apn_parcels.crs == {'init': 'epsg:4326'} # making sure input data file is in the right crs
    assert type(apn_parcels.geometry) == (gpd.geoseries.GeoSeries) #making sure i have a geom as geoseries
    logging.warning("PARCELS data - Loaded")
    return apn_parcels

def load_california_crops_data():
    crops = gpd.read_file("CROPS.geojson")
    assert crops.crs == {'init': 'epsg:4326'} # making sure input data file is in the right crs
    assert type(crops.geometry) == (gpd.geoseries.GeoSeries) #making sure i have a geom as geoseries
    logging.warning("CROP data - Loaded")
    return crops

def load_california_counties_data():
    counties = gpd.read_file("COUNTIES.geojson")
    assert counties.crs == {'init': 'epsg:4326'} # making sure input data file is in the right crs
    assert type(counties.geometry) == (gpd.geoseries.GeoSeries) #making sure i have a geom as geoseries
    logging.warning("COUNTIES data - Loaded")
    return counties

def get_geom_from_county_name(counties_data_frame, selected_county:str):
    selected_county_geom = counties_data_frame[counties_data_frame["NAME"] == selected_county]
    return selected_county_geom

def filter_data_by_county(data_frame_to_filter, county_geom):
    assert data_frame_to_filter.crs == county_geom.crs
    print("CRS Check - Passed")
    filtered_df = gpd.sjoin(data_frame_to_filter, county_geom, op='within')  # lets add
    return filtered_df

def filter_datasets_by_parcel_id(parcel_id: int, parcel_df):
    parcel_of_choice = parcel_df[parcel_df["APN"] == int(parcel_id)]
    return parcel_of_choice

def populate_an_aware_parcel(single_parcel_df, water_districts_df, crop_data_df):
    single_parcel_df = single_parcel_df.reset_index(drop=True) # we start here with the APN
    # we also need to add water districts information to single parcel df
    # need to reset index and to drop index_right that gets added during the sjoin process by gpd
    single_parcel_wtr_dist = gpd.sjoin(single_parcel_df, water_districts_df, how="left", op='intersects').reset_index(drop=True).drop(columns="index_right")
    # we need to add the crops data to the single parcel df in this case we dont need a buffer
    single_parcel_with_crop_data = gpd.sjoin(single_parcel_wtr_dist, crop_data_df, how="left", op='intersects').reset_index(drop=True).drop(columns="index_right")
    assert len(single_parcel_with_crop_data.columns) == len(crop_data_df.columns) + len(single_parcel_wtr_dist.columns) - 1
    return single_parcel_with_crop_data

def find_the_nearest_water_diversion(single_parcel_with_crop_data, diversion):
    #how about buffering around our APN block
    parcel_of_choice_buffered = parcel_of_choice.copy()
    parcel_of_choice_buffered["geometry"] = parcel_of_choice.buffer(0.1) #same as the raduis of CRS wich means its 0.5 degrees
    nearest_diversion_to_APN = gpd.sjoin(parcel_of_choice_buffered, DIVERSIONS, how="left", op='intersects').reset_index(drop=True) # keep right index need it later
    nearest_diversion_to_APN_df = pd.DataFrame(nearest_diversion_to_APN)
    diversion_points_around_my_apn = DIVERSIONS.loc[nearest_diversion_to_APN.index_right] # now we only extract the points from diversions that we want to view as pts
    diversion_points_around_my_apn.to_file("diversions_around_APN") #TODO REMOVE THIS CAUSE ITS ONLY FOR DEMONSTARTION
    # columns_to_drop = set(nearest_diversion_to_APN.columns)-set(['SPECIAL_AREA', 'HUC_12', 'HUC_8', 'HU_8_NAME', 'HU_12_NAME', "geometry"])
    # my_APN = nearest_diversion_to_APN.drop(columns=(list(columns_to_drop))) #todo DO WE WE NEED THIS NOW
    nearest_diversion_to_APN_df[nearest_diversion_to_APN_df["HU_12_NAME"].unique()]
    parcel_of_choice.to_file("parcel_of_choice")

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
    # check for inputs files and change directory
    check_for_conduit_input_data()
    #using path lib to let it figure out the difference paths based on OS instead of an if statement
    CONDUIT_INPUT_DATA_DIR = Path(os.getcwd() + '\data_logix\conduit_input_data') # should be testing this on linux by printing out the path and seeing if pathlib detects the os / path combo
    os.chdir(CONDUIT_INPUT_DATA_DIR) # no me gusta esto
    check_for_conduit_input_files()

    # lets load in all the needed data
    COUNTIES = load_california_counties_data()
    CROPS = load_california_crops_data()
    PARCELS = load_california_parcel_data()
    WATER_DISTRICTS = load_water_districts_data() # water districts
    DIVERSIONS = load_ewrims_diversion_data() # Points of Diversion (PODs) are locations where water is being drawn from a water source such as a stream or river.

    # using some the functions
    SAN_JOAQUIN_GEOM = get_geom_from_county_name(counties_data_frame=COUNTIES, selected_county="San Joaquin") #nice source of geom for the county but i dont use this somewhere else
    san_joaquin_crop = filter_data_by_county(data_frame_to_filter=CROPS, county_geom=SAN_JOAQUIN_GEOM)
    single_parcel_df = filter_datasets_by_parcel_id(parcel_id=17912900, parcel_df=PARCELS)
    parcel_of_choice = populate_an_aware_parcel(single_parcel_df, water_districts_df=WATER_DISTRICTS, crop_data_df=CROPS)
    parcel_of_choice.to_file("parcel_of_choice")
    parcel_of_choice_df = pd.DataFrame(parcel_of_choice)
    tell_me_more_about_my_parcel(parcel_of_choice_df)


