'''This file will be used to hit the DWR CIMIS API for agricultural data conditions such as precipitation,
ET, temp..etc'''

import requests

ABDEL_CIMIS_WEBAPI_KEY = "bb8106ae-9d2d-4333-9985-3df576adc14d" #you will need to create a free account at cimis and request an automated api key
ANOTHER_KEY = "fdf30e65-311e-4e02-856a-9b5adf86564d" #anohter API key because govt system is taking too long to register the created key
#CIMIS system requires an API key
START_DATE = "2010-01-01" #string of data
END_DATE = "2010-01-05"

URL = (f"http://et.water.ca.gov/api/data?appKey={ABDEL_CIMIS_WEBAPI_KEY}&targets=2,8,127&startDate={START_DATE}&endDate={END_DATE}")
URL = "http://et.water.ca.gov/api/data?appKey=fdf30e65-311e-4e02-856a-9b5adf86564d&targets=2,8,127&startDate=2010-01-01&endDate=2010-01-05"
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