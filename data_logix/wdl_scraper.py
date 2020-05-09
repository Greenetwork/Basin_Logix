import requests
from bs4 import BeautifulSoup

URL = "http://wdl.water.ca.gov/waterdatalibrary/groundwater/hydrographs/brr_hydro.cfm?CFGRIDKEY=50350"
r = requests.get(URL)

soup = BeautifulSoup(r.content, 'html.parser')
extraected = soup.find(string="_cf_gridColModel")
print(soup.prettify())
#
# soup.strings
# var _cf_gridColModel
# var _cf_gridData
# var _cf_gridDataModel