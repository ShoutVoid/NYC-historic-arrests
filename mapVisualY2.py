##mapVisualYears2

import sqlite3
import pandas as pd
import matplotlib as mpl
import matplotlib.style as mplstyle
import matplotlib.pyplot as plt
import descartes
import geopandas as gpd
from shapely.geometry import Point, Polygon
from pyproj import CRS

#dbname = 'cleanDataTiny.sqlite' #for testing / preview purposes
dbname = 'cleanData.sqlite' #ostensibly the entire dataset (over 5M rows!)

yr = input('Try a year between 2006 and 2022.\n') #The year to retrieve. Try a value between 2006 and 2022
if len(yr) < 1: exit()
try:
    yr = int(yr)
except:
    print('Invalid input. Run the program and try again.')
    exit()

mplstyle.use('fast')

try:
    with sqlite3.connect(dbname) as dbconnect:
        #df = pd.read_sql('SELECT arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group, perp_sex, perp_race, latitude, longitude FROM Arrest JOIN ArrestDate ON Arrest.arrest_date_id = ArrestDate.id JOIN PDDesc ON Arrest.pd_desc_id = PDDesc.id JOIN OffenseDesc ON Arrest.ofns_desc_id = OffenseDesc.id JOIN LawCode ON Arrest.law_code_id = LawCode.id JOIN LawCategory ON Arrest.law_cat_id = LawCategory.id JOIN ArrestBorough ON Arrest.arrest_boro_id = ArrestBorough.id JOIN AgeGroup ON Arrest.age_group_id = AgeGroup.id JOIN PerpSex ON Arrest.perp_sex_id = PerpSex.id JOIN PerpRace ON Arrest.perp_race_id = PerpRace.id;', dbconnect)
        df = pd.read_sql('SELECT arrest_date, ofns_desc, latitude, longitude FROM Arrest JOIN ArrestDate ON Arrest.arrest_date_id = ArrestDate.id JOIN OffenseDesc ON Arrest.ofns_desc_id = OffenseDesc.id;', dbconnect)
except:
    print('There\'s a problem with the sqlite database. Make sure to use \'getRaw.py\' followed by \'parseData.py\' to generate the \'cleanData.sqlite\' file.')
    exit()

try:
    nycMap = gpd.read_file('nyc.zip') #https://www.nyc.gov/site/planning/data-maps/open-data/dwn-digital-city-map.page
except:
    print('The shapefile couldn\'t be found or is otherwise invalid. Please use \'getSHP.py\' to retrieve it over the network, or check the URL in said Python script.')

df = df[ ( df.latitude < float(41) ) & ( df.latitude > float(40) ) ] #limit the crimes to be plotted in the map to ones that ocurred *within* NYC
df['arrest_date'] = pd.to_datetime(df['arrest_date']) #convert the string dates to a datetime object

gpd.options.display_precision = 6 #how precise are the coordinates (6 decimal places)
crs = CRS('epsg:4326') #initialize the Coordinate Reference System
geometry = gpd.points_from_xy(df.longitude, df.latitude) #create the Point objects for mapping coordinates
gdf = gpd.GeoDataFrame(df, geometry = geometry, crs = crs) #create the GeoPandas GeoDataFrame
gdf['Year'] = gdf['arrest_date'].dt.year #Make a year column for simpler retrieval
#print(gdf) #DEBUGGING
#print(gdf.loc[(gdf['Year'] == yr), ['ofns_desc']]) #DEBUGGING

fig, ax = plt.subplots(figsize=(20,10), layout='constrained') #drawing surfaces
base = nycMap.to_crs(crs).plot(ax=ax, alpha=0.1, color='black', zorder=0) #drawing NYC map and make sure it's the bottommost layer

failCount = 0 #for values not found etc.
try:
    ofnsPlot = gdf[gdf.Year == yr].plot(column = 'ofns_desc', ax = base, cmap = 'rainbow', legend = True, legend_kwds = { 'loc' : 'upper left', 'bbox_to_anchor' : (1.01, 1), 'ncol' : 2, 'fancybox' : True, 'shadow' : True, 'fontsize' : 7.5 }, markersize = 10, alpha = 0.1, marker = '.')
except:
    print('No data for this year.')
    failCount = failCount + 1
if (failCount >= 1): #it didn't find any offense in the given year; there's no point in an empty map, so it quits.
    print('There is nothing to draw.')
    exit()

plt.title('Crime in NYC (Offense Descriptions) -- ' + str(yr), loc='right') #give it a title
#plt.savefig( 'Crime in NYC (Offense Descriptions) -- ' + str(yr) ) #save it
plt.show() #display it
plt.close() #just in case: close it!
