'''
This file will be the import layer of the datasets saved on IPFS
then intention is that this layer will act a facilitator to future
functions that modify and enforce uniformity of the datasets
For now this file is importing dwr jsons
'''
import ipfshttpclient
import pandas as pd

HOST_LOCATION = '/ip4/127.0.0.1/tcp/5001/http'
FILE_HASH_DWR_WTR_DIST = "QmQ6ZvjG9XWUJYdYMNpHyDiy4sRa7vcvCbj7tL9GNHtSNW" # this is the hash for the water dist data
FILE_HASH_DWR_WTR_DIST_CSV = "Qmc7NHSBDinhk6ByUj77X5mYd5tmu3PtYXMnrBDTFJq93n"

https://ipfs.io/ipfs/QmQ6ZvjG9XWUJYdYMNpHyDiy4sRa7vcvCbj7tL9GNHtSNW?filename=dwr_water_districts_042520.geojson

def load_geojson_from_ipfs(host: str, file_to_load: str):
	client = ipfshttpclient.connect("136.25.136.150/127.0.0.1/tcp/5001/http") # connect to my local ipfs client
	loaded_json = client.get_json(file_to_load)  # this will pull the json file from ipfs
	loaded_json_df = pd.json_normalize(loaded_json["features"]) # this splits the : sep vals to columns
	pd.DataFrame.from_dict(loaded_json_df)
	return loaded_json_df

df = load_geojson_from_ipfs(host=HOST_LOCATION, file_to_load=FILE_HASH_DWR_WTR_DIST)