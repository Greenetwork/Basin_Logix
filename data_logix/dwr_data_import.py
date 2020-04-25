import ipfshttpclient
import os
import geopandas as gpd
from shapely.geometry import shape
import json
from pandas.io.json import json_normalize
import pandas as pd
import logging

HOST_LOCATION = '/ip4/127.0.0.1/tcp/5001/http'
FILE_HASH_DWR_WTR_DIST = "QmQ6ZvjG9XWUJYdYMNpHyDiy4sRa7vcvCbj7tL9GNHtSNW" # this is the hash for the water dist data

def load_geojson_from_ipfs(host: str, file_to_load: str, file_type: str):
	client = ipfshttpclient.connect(host) # connect to my local ipfs client
	loaded_file = client.cat(file_to_load) # this will pull the file from ipfs - it is a bytes class
	decoded_loaded_file = loaded_file.decode()
	if file_type is "JSON":
		decoded_loaded_df = pd.read_json(decoded_loaded_file)
	elif file_type is "CSV":
		decoded_loaded_df = pd.read_csv(decoded_loaded_file)
	elif file_type is "XLS":
		decoded_loaded_df = pd.read_excel(decoded_loaded_file)
	else:
		logging.warning("Please provide an acceptable file_type")
	return decoded_loaded_df

df = load_geojson_from_ipfs(host=HOST_LOCATION, file_to_load=FILE_HASH_DWR_WTR_DIST, file_type="JSON")