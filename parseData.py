#PARSEDATA.PY -- clean up the raw data info properly modeled tables (or so I hope)

import sqlite3

#constants
dbSource = 'rawData.sqlite'
#dbSource = 'rawDataTiny.sqlite'
dbClean = 'cleanData.sqlite'
#dbClean = 'cleanDataTiny.sqlite'

nullvalue = None

#sqlprep
with sqlite3.connect(dbClean) as dbconnect: #establish connection to the database using the context manager protocol
    dbcur = dbconnect.cursor() #create the cursor for SQL commands
    dbcur.executescript('''
    CREATE TABLE IF NOT EXISTS Arrest (
        id INTEGER NOT NULL PRIMARY KEY,
        arrest_key INTEGER UNIQUE,
        arrest_date_id INTEGER,
        pd_cd INTEGER,
        pd_desc_id INTEGER,
        ky_cd INTEGER,
        ofns_desc_id INTEGER,
        law_code_id INTEGER,
        law_cat_id INTEGER,
        arrest_boro_id INTEGER,
        arrest_precinct INTEGER,
        jurisdiction_code INTEGER,
        age_group_id INTEGER,
        perp_sex_id INTEGER,
        perp_race_id INTEGER,
        latitude FLOAT,
        longitude FLOAT
	);

    CREATE TABLE IF NOT EXISTS ArrestDate (
        id INTEGER NOT NULL PRIMARY KEY,
        arrest_date TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS date ON ArrestDate(arrest_date);

    CREATE TABLE IF NOT EXISTS PDDesc (
        id INTEGER NOT NULL PRIMARY KEY,
        pd_desc TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS pd_desc ON PDDesc(pd_desc);

    CREATE TABLE IF NOT EXISTS OffenseDesc (
        id INTEGER NOT NULL PRIMARY KEY,
        ofns_desc TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS offense ON OffenseDesc(ofns_desc);

    CREATE TABLE IF NOT EXISTS LawCode (
        id INTEGER NOT NULL PRIMARY KEY,
        law_code TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS law_code ON LawCode(law_code);

    CREATE TABLE IF NOT EXISTS LawCategory (
        id INTEGER NOT NULL PRIMARY KEY,
        law_cat_cd TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS law_cat ON LawCategory(law_cat_cd);

    CREATE TABLE IF NOT EXISTS ArrestBorough (
        id INTEGER NOT NULL PRIMARY KEY,
        arrest_boro TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS boro ON ArrestBorough(arrest_boro);

    CREATE TABLE IF NOT EXISTS AgeGroup (
        id INTEGER NOT NULL PRIMARY KEY,
        age_group TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS age ON AgeGroup(age_group);

    CREATE TABLE IF NOT EXISTS PerpSex (
        id INTEGER NOT NULL PRIMARY KEY,
        perp_sex TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS sex ON PerpSex(perp_sex);

    CREATE TABLE IF NOT EXISTS PerpRace (
        id INTEGER NOT NULL PRIMARY KEY,
        perp_race TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS race ON PerpRace(perp_race);
        ''')

#data parsing
with sqlite3.connect(dbSource) as dbConnectSource, sqlite3.connect(dbClean) as dbConnectClean: #establish connection to the database using the context manager protocol
    dbCursorSource = dbConnectSource.cursor() #create the cursor for SQL commands
    dbCursorClean = dbConnectClean.cursor()
    #print('It worked') #debug
    #print(dbCursorSource, dbCursorClean) #debug
    #arrest info -- individual tables
    ##ARREST DATE
    print('RETRIEVING ARREST DATES...')
    dbCursorSource.execute('''SELECT DISTINCT arrest_date FROM RawData ORDER BY arrest_date ASC''')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] == None: continue
        data = row[0]
        #print(data)
        dbCursorClean.execute( '''INSERT OR IGNORE INTO ArrestDate(arrest_date) VALUES (?)''', (data, ) )
    #dbConnectClean.commit()
    dbCursorClean.execute('''SELECT DISTINCT arrest_date FROM ArrestDate ORDER BY arrest_date ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED; HERE ARE THE FIRST 10 ROWS:', rowsPrint)
    ##ARREST BORO
    print('RETRIEVING ARREST BOROUGH...')
    dbCursorSource.execute('''SELECT DISTINCT arrest_boro FROM RawData ORDER BY arrest_boro ASC''')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] == None: continue
        data = row[0]
        dbCursorClean.execute( '''INSERT OR IGNORE INTO ArrestBorough(arrest_boro) VALUES (?)''', (data, ) )
    #dbConnectClean.commit()
    dbCursorClean.execute('''SELECT arrest_boro FROM ArrestBorough ORDER BY arrest_boro ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##PD DESC
    print('GATHERING PD_DESC (DESCRIPTION OF INTERNAL CLASSIFICATION CORRESPONDING WITH PD CODE (MORE GRANULAR THAN OFFENSE DESCRIPTION))')
    dbCursorSource.execute('''SELECT DISTINCT pd_desc FROM RawData ORDER BY pd_desc ASC''')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] == None: continue
        data = row[0]
        #print(data)
        dbCursorClean.execute( '''INSERT OR IGNORE INTO PDDesc(pd_desc) VALUES (?)''', (data, ) )
    #dbConnectClean.commit()
    dbCursorClean.execute('''SELECT pd_desc FROM PDDesc ORDER BY pd_desc ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##OFFENSE DESC
    print('GATHERING OFNS_DESC (DESCRIPTION OF INTERNAL CLASSIFICATION CORRESPONDING WITH KY CODE (MORE GENERAL CATEGORY THAN PD DESCRIPTION))')
    dbCursorSource.execute('''SELECT DISTINCT ofns_desc FROM RawData ORDER BY ofns_desc ASC''')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] == None: continue
        data = row[0]
        #print(data)
        dbCursorClean.execute( '''INSERT OR IGNORE INTO OffenseDesc(ofns_desc) VALUES (?)''', (data, ) )
    #dbConnectClean.commit()
    dbCursorClean.execute('''SELECT DISTINCT ofns_desc FROM OffenseDesc ORDER BY ofns_desc ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##LAW CATEGORY
    dbCursorSource.execute('SELECT DISTINCT law_cat_cd FROM RawData ORDER BY law_cat_cd ASC')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        data = row[0]
        if data is None: continue
        if data == '9': data = 'I'
        dbCursorClean.execute( '''INSERT OR IGNORE INTO LawCategory(law_cat_cd) VALUES (?)''', (data,) )
    dbCursorClean.execute('''SELECT law_cat_cd FROM LawCategory ORDER BY law_cat_cd ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##LawCode
    dbCursorSource.execute('SELECT DISTINCT law_code FROM RawData ORDER BY law_code ASC')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        data = row[0]
        if data is None: continue
        dbCursorClean.execute( '''INSERT OR IGNORE INTO LawCode(law_code) VALUES (?)''', (data, ) )
    dbCursorClean.execute('''SELECT law_code FROM LawCode ORDER BY law_code ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##Perp Sex
    dbCursorSource.execute('SELECT DISTINCT perp_sex FROM RawData ORDER BY perp_sex ASC')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] is None: continue
        data = row[0]
        #print(data)
        dbCursorClean.execute( '''INSERT OR IGNORE INTO PerpSex(perp_sex) VALUES (?)''', (data, ) )
    dbCursorClean.execute('''SELECT perp_sex FROM PerpSex ORDER BY perp_sex ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##Perp RACE
    dbCursorSource.execute('SELECT DISTINCT perp_race FROM RawData ORDER BY perp_race ASC')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] is None: continue
        data = row[0]
        #print(data)
        dbCursorClean.execute( '''INSERT OR IGNORE INTO PerpRace(perp_race) VALUES (?)''', (data, ) )
    dbCursorClean.execute('''SELECT perp_race FROM PerpRace ORDER BY perp_race ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)
    ##AGE GROUP
    dbCursorSource.execute('SELECT DISTINCT age_group FROM RawData ORDER BY age_group ASC')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        if row[0] is None: continue
        if row[0].isdigit(): continue
        data = row[0]
        #print(data)
        dbCursorClean.execute( '''INSERT OR IGNORE INTO AgeGroup(age_group) VALUES (?)''', (data, ) )
    dbCursorClean.execute('''SELECT age_group FROM AgeGroup ORDER BY age_group ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('\nTHE TABLE IS NOW POPULATED:', rowsPrint)

    #POPULATING MAIN ARREST TABLE
    print('\nPOPULATING THE MAIN ARREST TABLE...')
    dbCursorSource.execute('''SELECT arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group, perp_sex, perp_race, latitude, longitude FROM RawData ORDER BY arrest_key ASC''')
    sourceRows = dbCursorSource.fetchall()
    for row in sourceRows:
        arrest_key = arrest_date = pd_cd = pd_desc = ky_cd = ofns_desc = law_code = law_cat_cd = arrest_boro = arrest_precinct = jurisdiction_code = age_group = perp_sex = perp_race = latitude = longitude = None
        if row[0] == None: continue
        arrest_key = row[0]
        arrest_date = row[1]
        pd_cd = row[2]
        pd_desc = row[3]
        ky_cd = row[4]
        ofns_desc = row[5]
        law_code = row[6]
        law_cat_cd = row[7]
        arrest_boro = row[8]
        arrest_precinct = row[9]
        jurisdiction_code = row[10]
        age_group = row[11]
        perp_sex = row[12]
        perp_race = row[13]
        latitude = row[14]
        longitude = row[15]
        #print(data)
        #check if this data is already parsed in their respective tables (if applicable), and retrieve the corresponding IDs. Otherwise keep null.
        print('\nCLEANING DATA...')
        ##ARREST DATE
        dbCursorClean.execute('SELECT id FROM ArrestDate WHERE arrest_date = (?)', (arrest_date,))
        try:
            arrest_date = dbCursorClean.fetchone()[0]
        except:
            arrest_date = nullvalue
        ##PD DESC
        dbCursorClean.execute('SELECT id FROM PDDesc WHERE pd_desc = (?)', (pd_desc,))
        try:
            pd_desc = dbCursorClean.fetchone()[0]
        except:
            pd_desc = nullvalue
        ##OFFENSE DESC
        dbCursorClean.execute('SELECT id FROM OffenseDesc WHERE ofns_desc = (?)', (ofns_desc,))
        try:
            ofns_desc = dbCursorClean.fetchone()[0]
        except:
            ofns_desc = nullvalue
        ##LAW CODE
        dbCursorClean.execute('SELECT id FROM LawCode WHERE law_code = (?)', (law_code,))
        try:
            law_code = dbCursorClean.fetchone()[0]
        except:
            law_code = nullvalue
        ##LAW CATEGORY
        dbCursorClean.execute('SELECT id FROM LawCategory WHERE law_cat_cd = (?)', (law_cat_cd,))
        try:
            law_cat_cd = dbCursorClean.fetchone()[0]
        except:
            law_cat_cd = nullvalue
        ##PERP RACE
        dbCursorClean.execute('SELECT id FROM PerpRace WHERE perp_race = (?)', (perp_race,))
        try:
            perp_race = dbCursorClean.fetchone()[0]
        except:
            perp_race = nullvalue
        ##AGE GROUP
        dbCursorClean.execute('SELECT id FROM AgeGroup WHERE age_group = (?)', (age_group,))
        try:
            age_group = dbCursorClean.fetchone()[0]
        except:
            age_group = nullvalue
        ##PERP SEX
        dbCursorClean.execute('SELECT id FROM PerpSex WHERE perp_sex = (?)', (perp_sex,))
        try:
            perp_sex = dbCursorClean.fetchone()[0]
        except:
            perp_sex = nullvalue
        #arrest borough
        dbCursorClean.execute('SELECT id FROM ArrestBorough WHERE arrest_boro = (?)', (arrest_boro,))
        try:
            arrest_boro = dbCursorClean.fetchone()[0]
        except:
            arrest_boro = nullvalue
        #latitude & longitude
        try:
            latitude = '%.6f' % latitude
            longitude = '%.6f' % longitude
        except:
            latitude = nullvalue
            longitude = nullvalue
        #final commit
        dbCursorClean.execute( '''INSERT OR IGNORE INTO Arrest(arrest_key, arrest_date_id, pd_cd, pd_desc_id, ky_cd, ofns_desc_id, law_code_id, law_cat_id, arrest_boro_id, arrest_precinct, jurisdiction_code, age_group_id, perp_sex_id, perp_race_id, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group, perp_sex, perp_race, latitude, longitude ) )
    #dbConnectClean.commit()
    dbCursorClean.execute('''SELECT arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code, law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group, perp_sex, perp_race, latitude, longitude FROM Arrest JOIN ArrestDate ON Arrest.arrest_date_id = ArrestDate.id JOIN PDDesc ON Arrest.pd_desc_id = PDDesc.id JOIN OffenseDesc ON Arrest.ofns_desc_id = OffenseDesc.id JOIN LawCode ON Arrest.law_code_id = LawCode.id JOIN LawCategory ON Arrest.law_cat_id = LawCategory.id JOIN ArrestBorough ON Arrest.arrest_boro_id = ArrestBorough.id JOIN AgeGroup ON Arrest.age_group_id = AgeGroup.id JOIN PerpSex ON Arrest.perp_sex_id = PerpSex.id JOIN PerpRace ON Arrest.perp_race_id = PerpRace.id ORDER BY arrest_key ASC LIMIT 10''')
    rowsPrint = dbCursorClean.fetchall()
    print('The Arrest table is now populated.', rowsPrint)
