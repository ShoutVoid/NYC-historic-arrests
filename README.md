# NYC-historic-arrests
A collection of scripts to retrieve [historic (2006-2022) arrest data](https://data.cityofnewyork.us/Public-Safety/NYPD-Arrests-Data-Historic-/8h9b-rp9u) from [NYC's OpenData](https://opendata.cityofnewyork.us/) website via its API into a local SQLite file for parsing, cleaning and interpreting in various ways (such as drawing a NYC map and placing markers), using Python libraries such as [Pandas](https://pandas.pydata.org/), [GeoPandas](https://geopandas.org/en/stable/index.html) and [Matplotlib](https://matplotlib.org/).

Set up a Python virtual environment with the aforementioned libraries and give them a go; use getSHP.py to retrieve the New York City shapefiles, followed by getRaw.py, parseData.py and any of the visualizations (barVisual.py, mapVisualY.py and maptVisualY2.py).

![Crime in NYC (Law Categories) -- 2006](https://github.com/ShoutVoid/NYC-historic-arrests/assets/167651258/04ad95aa-bdc0-4b54-8dc5-a836c11df2fd)

![Crime in NYC (Law Categories) -- 2020](https://github.com/ShoutVoid/NYC-historic-arrests/assets/167651258/5464a85f-bd40-49a5-b554-8a2f35e90b04)

![Arrests in NYC -- 2006-2022  Bar graph](https://github.com/ShoutVoid/NYC-historic-arrests/assets/167651258/a88cf9d5-587a-4cb6-b6d7-f596a7c7802e)
