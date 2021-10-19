""" Ingest the date to  b"""

import fiona
import os
import geopandas as gpd
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import time
import re

from shapely.geometry import Polygon, MultiPolygon, shape, Point

'42-1'

def convert_3D_2D(geometry):
    '''
    Takes a GeoSeries of 3D Multi/Polygons (has_z) and returns a list of 2D Multi/Polygons
    '''
    new_geo = []
    for p in geometry:
        if p.has_z:
            if p.geom_type == 'Polygon':
                lines = [xy[:2] for xy in list(p.exterior.coords)]
                new_p = Polygon(lines)
                new_geo.append(new_p)
            elif p.geom_type == 'MultiPolygon':
                new_multi_p = []
                for ap in p:
                    lines = [xy[:2] for xy in list(ap.exterior.coords)]
                    new_p = Polygon(lines)
                    new_multi_p.append(new_p)
                new_geo.append(MultiPolygon(new_multi_p))
    return new_geo


def ingest_dwr_data(county):
    os.chdir("/Volumes/GoogleDrive/.shortcut-targets-by-id/1Pys6uDFGgKrr67FLOqZ-dQ3tnmJ9Vg6G/"
             "Basin_Logix_Root/BLX_BUIDL_2021/product_development/CA_CROP_and_Parcel/")
    load_dotenv('/Users/abdel/Documents/basinlogix/Basin_Logix/.env')
    db_uri = os.getenv("DB_CREDS")
    # LOAD ALL FILES
    print("STARTED LOADING ALL FILES")
    gdf_counties = gpd.read_file('raw_data/ca-county-boundaries/CA_Counties/CA_Counties_TIGER2016.dbf',
                                 driver='FileGDB',
                                 layer='CA_Counties_TIGER2016',
                                 # rows=100
                                 )
    print(f"COUNTIES CONTAINS {len(gdf_counties)} ROWS")
    gdf_kern_county = gdf_counties.loc[gdf_counties['NAME'] == county]

    gdf_crop = gpd.read_file('raw_data/i15_crop_mapping_2018_gdb/i15_crop_mapping_2018.gdb',
                             driver='FileGDB',
                             layer='i15_Crop_Mapping_2018',
                             mask=gdf_kern_county,
                             # rows=200
                             )
    print(f"CROP CONTAINS {len(gdf_crop)} ROWS")
    gdf_apn = gpd.read_file('raw_data/Parcels_CA_2014.gdb/',
                            driver='FileGDB',
                            layer='CA_PARCELS_STATEWIDE',
                            mask=gdf_kern_county,
                            # rows=200
                            )
    print(f"APN CONTAINS {len(gdf_apn)} ROWS")

    gdf_gsa = gpd.read_file('raw_data/submittedgsa/',
                            driver='FileGDB',
                            layer='GSA_Master',
                            mask=gdf_kern_county,
                            # rows=200
                            )
    print(f"GSA CONTAINS {len(gdf_gsa)} ROWS")

    gdf_118 = gpd.read_file('raw_data/B118_2018_GISdata/Geodatabase/B118_v6-1.gdb',
                            driver='FileGDB',
                            layer='i08_B118_v6_1',
                            mask=gdf_kern_county,
                            # rows=200
                            )
    print(f"118 CONTAINS {len(gdf_118)} ROWS")

    meta_data_dict = pd.read_excel('crop_metadata.xlsx',
                                   engine='openpyxl',
                                   sheet_name='formatted',
                                   header=None, names=['key', 'value']).set_index('key').T.to_dict('records')[0]

    print("DONE LOADING ALL FILES")
    # set CRS
    gdf_crop.to_crs(3310, inplace=True)
    gdf_counties.to_crs(3310, inplace=True)
    gdf_apn.to_crs(3310, inplace=True)
    gdf_gsa.to_crs(3310, inplace=True)
    gdf_118.to_crs(3310, inplace=True)
    print("DONE CRS TRANSFORMATION ALL FILES")

    # JOINING
    print("STARTING JOINS")
    gdf_combo = gpd.sjoin(gdf_crop, gdf_apn, how='inner', op='intersects')
    gdf_over = gpd.overlay(gdf_crop, gdf_apn, how='intersection')
    gdf_over['area_overlap'] = gdf_over.geometry.area
    gdf_over_max = gdf_over.loc[gdf_over.groupby('UniqueID')['area_overlap'].agg(pd.Series.idxmax)][['UniqueID',
                                                                                                     'PARNO',
                                                                                                     'area_overlap']]
    gdf_combo_max_area = gdf_combo.merge(gdf_over_max, left_on=['UniqueID',
                                                                'PARNO'], right_on=['UniqueID',
                                                                                    'PARNO'])

    gdf_combo_max_area['crop2018'] = gdf_combo_max_area['CROPTYP2'].map(meta_data_dict)
    gdf_combo_max_area.drop(columns=["index_right"], inplace=True)
    gdf_combo_118 = gpd.sjoin(gdf_combo_max_area, gdf_118, how='inner',
                              op='intersects')
    gdf_over = gpd.overlay(gdf_combo_max_area, gdf_118,
                           how='intersection')
    gdf_over['area_overlap'] = gdf_over.geometry.area
    gdf_over_max = gdf_over.loc[gdf_over.groupby('UniqueID')['area_overlap'].agg(pd.Series.idxmax)][['UniqueID',
                                                                                                     'Basin_Subbasin_Number',
                                                                                                     'area_overlap']]
    gdf_combo_118_max_area = gdf_combo_118.merge(gdf_over_max, on=['UniqueID', 'Basin_Subbasin_Number'])
    gdf_combo_118_max_area = gdf_combo_118_max_area[['UniqueID',
                                                     'geometry', 'PARNO', 'County', 'Basin_Subbasin_Number',
                                                     'crop2018', 'REGION', 'ACRES']]
    gdf_combo_GSA = gpd.sjoin(gdf_combo_118_max_area, gdf_gsa, how='inner', op='intersects')
    gdf_over = gpd.overlay(gdf_combo_118_max_area, gdf_gsa, how='intersection')
    gdf_over['area_overlap'] = gdf_over.geometry.area
    gdf_over_max = gdf_over.loc[gdf_over.groupby('UniqueID')['area_overlap'].agg(pd.Series.idxmax)][['UniqueID',
                                                                                                     'GSA_ID',
                                                                                                     'area_overlap']]
    gdf_combo_118_GSA_max_area = gdf_combo_GSA.merge(gdf_over_max, left_on=['UniqueID',
                                                                            'GSA_ID'], right_on=['UniqueID',
                                                                                                 'GSA_ID'])
    final_df = gdf_combo_118_GSA_max_area.explode()[['geometry', 'PARNO',
                                                     'County', 'GSA_ID', 'DWR_GSA_ID', 'GSA_Name',
                                                     'Basin_Subbasin_Number', 'crop2018', 'REGION', 'ACRES']]
                                                     # 'UniqueID'
    print("CLEANING APN COLUMN")
    final_df["PARNO_COPY"] = [re.sub("[^0-9^.]", "", string).replace("-", "").replace(" ", "")
                              for string in list(final_df["PARNO"])]
    final_df["PARNO_COPY"] = ["00000" if x == "" else x for x in list(final_df["PARNO_COPY"])]
    final_df["PARNO_COPY"] = ["00000" if x == "42-1" else x for x in list(final_df["PARNO_COPY"])]
    final_df["PARNO_COPY"] = final_df["PARNO_COPY"].astype(int)

    print("DONE WITH ALL JOINS")
    final_df.rename(columns={
        # 'UniqueID': 'id',  # drop not really needed or map to "id"
        'geometry': 'geometry',  # 4326
        'PARNO': 'apn_chr',  # APN STR
        'PARNO_COPY': "apn",  # APN INT
        'County': 'county',
        'GSA_ID': 'gsa_id',  # todo: NEW COL NEED TO BE ADDED (int)
        'DWR_GSA_ID': 'dwr_gsa_id',  # todo: NEW COL NEED TO BE ADDED (int)
        'Basin_Subbasin_Number': 'dwr_gsa_basin_subbasin_num',  # todo: NEW COL NEED TO BE ADDED (str)
        'GSA_Name': 'agencyname',  # migration from old to new data
        'crop2018': 'crop2016',
        'REGION': 'region',
        'ACRES': 'acres'}, inplace=True)

    final_df = final_df.astype(dtype={
        "apn_chr": str,
        "apn": int,
        "county": str,
        "gsa_id": int,
        "dwr_gsa_id": int,
        "dwr_gsa_basin_subbasin_num": str,
        "agencyname": str,
        "crop2016": str,
        "region": str,
        "acres": float,
        # "id": int,
    })
    print(f"FINAL DATAFRAME CONTAINS {len(final_df)} ROWS")
    final_df.reset_index(drop=True, inplace=True)
    final_df["geometry"] = convert_3D_2D(final_df.geometry)
    final_df.to_crs(4326, inplace=True)
    blx_engine = create_engine(db_uri)
    print(f"STARTED WRITING DATA TO DB FOR {county}")
    final_df.to_postgis('blx_consolidated_apn',
                        blx_engine,
                        if_exists='append',
                        chunksize=250)
    print(f"DONE WRITING DATA TO DB FOR {county}")


if __name__ == "__main__":
    list_of_counties = [
                        'Alameda',
                        'Alpine',
                        'Amador',
                        'Butte',
                        'Calaveras',
                        'Contra Costa',
                        'Del Norte',
                        'El Dorado',
                        'Fresno',
                        'Glenn',
                        'Humboldt',
                        'Imperial',
                        'Kern',
                        'Kings',
                        'Lake',
                        'Lassen',
                        'Los Angeles', #
                        'Madera',
                        'Marin',
                        'Mendocino',
                        'Merced',
                        'Mono',
                        'Monterey',
                        'Napa',
                        'Nevada',
                        'Orange',
                        'Placer',
                        'Riverside',
                        'Sacramento',
                        'San Benito',
                        'San Bernardino',
                        'San Diego',
                        'San Joaquin',
                        'San Mateo',
                        'Santa Barbara',
                        'Santa Clara',
                        'Santa Cruz',
                        'Shasta',
                        'Sierra',
                        'Solano',
                        'Sonoma',
                        'Stanislaus',
                        'Sutter',
                        'Tehama',
                        'Trinity',
                        'Tulare',
                        'Tuolumne',
                        'Ventura',
                        'Yolo',
                        'Yuba'
    ]
    failed_runs = {}
    for county_str in tqdm(list_of_counties):
        print(f"STARTING WITH {county_str}")
        try:
            start_time = time.time()
            ingest_dwr_data(county=county_str)
            print(f"DONE WITH {county_str}")
            print("--- %s seconds ---" % (time.time() - start_time))
        except Exception as e:
            failed_runs[county_str] = e
            print(f"FAILED with {county_str} BECAUSE OF {e}")
            continue
