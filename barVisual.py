#barVisual.py

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

dbname='cleanData.sqlite'

try:
    with sqlite3.connect(dbname) as dbconnect:
        #df = pd.read_sql('SELECT arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group, perp_sex, perp_race, latitude, longitude FROM Arrest JOIN ArrestDate ON Arrest.arrest_date_id = ArrestDate.id JOIN PDDesc ON Arrest.pd_desc_id = PDDesc.id JOIN OffenseDesc ON Arrest.ofns_desc_id = OffenseDesc.id JOIN LawCode ON Arrest.law_code_id = LawCode.id JOIN LawCategory ON Arrest.law_cat_id = LawCategory.id JOIN ArrestBorough ON Arrest.arrest_boro_id = ArrestBorough.id JOIN AgeGroup ON Arrest.age_group_id = AgeGroup.id JOIN PerpSex ON Arrest.perp_sex_id = PerpSex.id JOIN PerpRace ON Arrest.perp_race_id = PerpRace.id;', dbconnect)
        df = pd.read_sql('SELECT arrest_date, law_cat_cd FROM Arrest JOIN ArrestDate ON Arrest.arrest_date_id = ArrestDate.id JOIN LawCategory ON Arrest.law_cat_id = LawCategory.id;', dbconnect)
except:
    print('There\'s a problem with the sqlite database. Make sure to use \'getRaw.py\' followed by \'parseData.py\' to generate the \'cleanData.sqlite\' file.')
    exit()

df['arrest_date'] = pd.to_datetime(df['arrest_date']) #convert the string dates to a datetime object
df['Year'] = df['arrest_date'].dt.year #Make a year column for simpler retrieval

fig, ax = plt.subplots(figsize=(20,10), layout='constrained') #drawing surfaces
#plt.figure(figsize=(1280*px,720*px), layout = 'constrained')
barPlot = df.groupby(df.Year)['law_cat_cd'].value_counts(dropna=False).plot(kind = 'bar', color = ['royalblue', 'crimson', 'forestgreen', 'darkorange'] )
#plt.grid(True)
plt.xlabel('Year, Offense Category')
plt.ylabel('N. of Arrests')
plt.title('Arrests in NYC -- 2006-2022')
plt.xticks(rotation=45)

#plt.savefig('Crime in NYC -- 2006-2022 [Bar graph]')
plt.show()
plt.close() #just in case, close it
