'''This file will be used to hit the DWR CIMIS API for agricultural data conditions such as precipitation,
ET, temp..etc
'''

import requests

#cimis data
# WSN data by station number(s)
# WSN or SCS data by zip code(s) < focus on this
# SCS data by coordinate in decimal degrees < focus on this
# SCS data by street address(es)
# the overlap between waeather station network data requests and CIMIS data is a daily data by Zip Code request
# this means that most of the requests will need to take place by a zip code
#

ABDEL_CIMIS_WEBAPI_KEY = "f8fc29ac-0aba-40d5-ae8f-4e3c3320f3ba" #you will need to create a free account at cimis and request an automated api key
#CIMIS system requires an API key
START_DATE = "2010-01-01" #string of date
END_DATE = "2010-01-05"
DATA_ITEMS = "day-asce-eto,day-eto,day-precip"
UNIT_OF_MEASURE = "M"
ZIP_CODE = '95823'
PRIORITIZE_SCS_FLAG = "Y"

spatial_zip_code_list =
station_zip_code_list = requests.get("http://et.water.ca.gov/api/data?appKey=f8fc29ac-0aba-40d5-ae8f-4e3c3320f3ba&targets=19122&startDate=2019-01-01&endDate=2019-01-05").content

def CimisConnect:
    def get_data (api_key:str, start_date: str, zip_code: str, end_date: str, unit_of_measure: str, scs_priority_flag: str):
        URL = (f"http://et.water.ca.gov/api/data?appKey={api_key}"
               f"&targets={zip_code}"
               f"&startDate={start_date}"
               f"&endDate={end_date}"
               f"&dataItems={DATA_ITEMS}"
               f"&unitOfMeasure={unit_of_measure};prioritizeSCS={scs_priority_flag}")
        response = requests.get(URL)
        content = response.content
        return content



response = requests.get(URL)
response.content

# If the response was successful, no Exception will be raised
response.raise_for_status()
#
# except HTTPError as http_err:
# print(f'HTTP error occurred: {http_err}')  # Python 3.6
# except Exception as err:
# print(f'Other error occurred: {err}')  # Python 3.6
# else:
# print('Success!')
#

# CIMIS AG CLIMATE/WEATHER DATA
cimis_station_locations_df = pd.read_excel("dwr_cimis_stations_list_042520.xlsx") # this will be helpful when we need to pull using the ICIMS API
#will drop the rows that contain NA in the Lat or Long cols
cimis_station_locations_df = cimis_station_locations_df.dropna(subset=["Longitude", "Latitude"])
#georeferencing the df of the cimis stations
cimis_station_locations_gdf = gpd.GeoDataFrame(cimis_station_locations_df,
                                               geometry = gpd.points_from_xy(cimis_station_locations_df.Longitude,
                                                                             cimis_station_locations_df.Latitude))