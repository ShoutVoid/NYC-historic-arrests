##getSHP.py -- get those pesky shapefiles!

import requests

##https://data.cityofnewyork.us/City-Government/Digital-City-Map-Shapefile/m2vu-mgzw
##https://www.nyc.gov/site/planning/data-maps/open-data/dwn-digital-city-map.page
url = 'https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/dcm_20240331shp.zip'

response = requests.get(url, stream=True)
with open('nyc.zip', "wb") as f:
    for chunk in response.iter_content(chunk_size=512):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
