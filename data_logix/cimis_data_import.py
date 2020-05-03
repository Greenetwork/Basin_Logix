'''This file will be used to hit the DWR CIMIS API for agricultural data conditions such as precipitation,
ET, temp..etc'''

import requests

#cimis data

ABDEL_CIMIS_WEBAPI_KEY = "bb8106ae-9d2d-4333-9985-3df576adc14d" #you will need to create a free account at cimis and request an automated api key
ANOTHER_KEY = "fdf30e65-311e-4e02-856a-9b5adf86564d" #anohter API key because govt system is taking too long to register the created key
#CIMIS system requires an API key
START_DATE = "2010-01-01" #string of data
END_DATE = "2010-01-05"

URL = (f"http://et.water.ca.gov/api/data?appKey={ANOTHER_KEY}&targets=2,8,127&startDate={START_DATE}&endDate={END_DATE}")
#URL = "http://et.water.ca.gov/api/data?appKey=fdf30e65-311e-4e02-856a-9b5adf86564d&targets=2,8,127&startDate=2010-01-01&endDate=2010-01-05"
response = requests.get(URL)
response.content

# If the response was successful, no Exception will be raised
response.raise_for_status()

except HTTPError as http_err:
print(f'HTTP error occurred: {http_err}')  # Python 3.6
except Exception as err:
print(f'Other error occurred: {err}')  # Python 3.6
else:
print('Success!')


# CIMIS AG CLIMATE/WEATHER DATA
cimis_station_locations_df = pd.read_excel("dwr_cimis_stations_list_042520.xlsx") # this will be helpful when we need to pull using the ICIMS API
#will drop the rows that contain NA in the Lat or Long cols
cimis_station_locations_df = cimis_station_locations_df.dropna(subset=["Longitude", "Latitude"])
#georeferencing the df of the cimis stations
cimis_station_locations_gdf = gpd.GeoDataFrame(cimis_station_locations_df,
                                               geometry = gpd.points_from_xy(cimis_station_locations_df.Longitude,
                                                                             cimis_station_locations_df.Latitude))