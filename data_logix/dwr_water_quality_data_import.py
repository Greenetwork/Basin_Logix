import urllib
import requests
import urllib.request
import pandas as pd
import json
from pandas.io.json import json_normalize


# def pull_dwr_water_quality_data():
#     def create_url_for_request():

URL = "https://data.cnra.ca.gov/dataset/3f96977e-2597-4baa-8c9b-c433cea0685e/resource/a9e7ef50-54c3-4031-8e44-aa46f3c660fe/download/lab-results.csv"
with urllib.request.urlopen(URL) as url:
    s = url.read()
    urllib.request.urlretrieve(URL, 'lab-results.csv')
    print(type(s))
    s = s.decode()
    print(type(s))
    dic = json.loads(s)

    df = pd.DataFrame.from_dict(s, orient='columns')

    df = pd.read_json(url)
    s_df = pd.json_normalize(s)
    # I'm guessing this would output the html source code ?
    print(s)


url = 'https://data.ca.gov/api/3/action/datastore_search?resource_id=084f4e83-3d44-42ed-badb-ab52ee74ce5a&limit=5&q=title:jones'
fileobj = urllib.urlopen(url)
response = requests.get(url)
response.content

# If the response was successful, no Exception will be raised
response.raise_for_status()
print fileobj.read()