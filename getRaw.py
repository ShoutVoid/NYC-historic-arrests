#GETRAW.PY -- hits an API that provides a JSON file and makes a database of the raw data, no cleanup or fixup.

import urllib.request, urllib.parse, urllib.error
#import ssl
import json
import sqlite3

#api url
baseUrl = 'https://data.cityofnewyork.us/resource/8h9b-rp9u.json'
dbname = 'rawData.sqlite'
#dbname = 'rawDataTiny.sqlite' #testing
maxRetrieve = 50000 #the max amount of records to be retrieved in one sitting. Depending on the API ver, it might be limited to 1.000 (for the $offset=) and/or 50.000 (for the $limit=), or unlimited; with the way this program works, setting it too high (besides pummeling the API endpoint, maybe) will greatly increase the RAM usage.

#functions
def getData(url):
    #with urllib.request.urlopen(url, None, 30, context=ctx) as uh:
    with urllib.request.urlopen(url) as uh:
            data = json.loads( uh.read().decode() ) #returns a list of dictionaries
    return data

# Ignore SSL certificate errors
#ctx = ssl.create_default_context()
#ctx.check_hostname = False
#ctx.verify_mode = ssl.CERT_NONE

#sqlprep
with sqlite3.connect(dbname) as dbconnect: #establish connection to the database using the context manager protocol
    dbcur = dbconnect.cursor() #create the cursor for SQL commands
    dbcur.executescript('''
        CREATE TABLE IF NOT EXISTS RawData (
            id                INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            arrest_key        INTEGER NOT NULL UNIQUE,
            arrest_date       TEXT,
            pd_cd             INTEGER,
            pd_desc           TEXT,
            ky_cd             INTEGER,
            ofns_desc         TEXT,
            law_code          TEXT,
            law_cat_cd        TEXT,
            arrest_boro       TEXT,
            arrest_precinct   INTEGER,
            jurisdiction_code INTEGER,
            age_group         TEXT,
            perp_sex          TEXT,
            perp_race         TEXT,
            latitude          FLOAT,
            longitude         FLOAT
        );
        ''')

#main
url = baseUrl
startParse = None
limit = 0

while True:
    #user input
    userInput = input('How many rows to query? Quit with \'-q\'.\n')
    if (userInput == '-q'): break
    if ( len(userInput) < 1):
        print('Input error: No input. Try again.')
        continue
    try:
        limit = int( userInput )
    except:
        print('Input error: Not a number. Try again.')
        continue
    if ( limit <= 0 ):
        print('Input error: Zero or less rows to query. Try again.')
        continue

    loops = limit // maxRetrieve
    if(loops < 1 or limit % maxRetrieve != 0):
        loops = loops + 1
    if (loops > 1 and limit % maxRetrieve == 0):
        limit = maxRetrieve
    while ( loops > 0 ):
        print('The program will run for', loops, 'loops, for an user input of', userInput)
        if (loops == 1 and limit % maxRetrieve != 0):
            limit = limit % maxRetrieve
        with sqlite3.connect(dbname) as dbconnect: #establish connection to the database using the context manager protocol
            dbcur = dbconnect.cursor() #create the cursor for SQL commands
            #to get to the last existing row in the SQL database
            try:
                dbcur.execute('SELECT max(id) FROM RawData')
                row = dbcur.fetchone()
                if row is None: startParse = 0 #if a row doesn't exist (i.e. blank DB file), start parsing from 0
                else: startParse = row[0] #the id column; if a row exists, start parsing from that id-column onwards
            except:
                startParse = 0 #just in case: start from 0
            if (startParse == None): #just in case: start from 0
                startParse = 0

            url = baseUrl + '?$limit=' + str(limit) + '&$offset=' + str(startParse) #https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more than-1000-rows-of-a-dataset
            print(url)
            print('Querying', limit, 'records, starting at record n.', startParse)

            try:
                d = getData(url)
            except KeyboardInterrupt:
                print('\nThe user interrupted the program.')
                exit()
            except:
                print('Something\'s wrong with the URL! Please check it and try again.')
                print(url)
                exit()

            recordCounter = 0
            print('Writing to disk...', dbname)
            for i in d:
                arrest = { 'arrest_key' : None, 'arrest_date' : None, 'pd_cd' : None, 'pd_desc' : None, 'ky_cd' : None, 'ofns_desc' : None, 'law_code' : None, 'law_cat_cd' : None, 'arrest_boro' : None, 'arrest_precinct' : None, 'jurisdiction_code' : None, 'age_group' : None, 'perp_sex' : None, 'perp_race' : None, 'latitude' : None, 'longitude' : None } #a dict with "sane defaults" for all the rows we're interested in.
                for k,v in i.items():
                    if k.endswith('_coord_cd'): continue
                    if k == 'lon_lat': continue
                    if k.startswith(':'): continue
                    arrest[k] = v
                print('Will commit the following arrest:', arrest['arrest_key'])
                dbcur.execute('INSERT OR IGNORE INTO RawData (arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group, perp_sex, perp_race, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', ( arrest['arrest_key'], arrest['arrest_date'], arrest['pd_cd'], arrest['pd_desc'], arrest['ky_cd'], arrest['ofns_desc'], arrest['law_code'], arrest['law_cat_cd'], arrest['arrest_boro'], arrest['arrest_precinct'], arrest['jurisdiction_code'], arrest['age_group'], arrest['perp_sex'], arrest['perp_race'], arrest['latitude'], arrest['longitude'] ) )
                recordCounter = recordCounter + 1
            if (recordCounter == 0):
                print('No more records in this database.')
            else:
                print('Finished retrieving', recordCounter, 'records.')
            #dbconnect.commit()
        d.clear()
        try:
            arrest.clear()
        except:
            #print('Error clearing the "arrest" dict. Perhaps it doesn\'t exist.')
            continue
        loops = loops - 1
