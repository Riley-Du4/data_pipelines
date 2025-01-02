import os
import pandas as pd
import psycopg2
import numpy as np
path = "C:/Users/Riley/Dropbox/5330corgis"
ls = os.listdir(path)
for file in ls:
    print(file)
conn = psycopg2.connect(
    database="corgirace", # Create this database first (e.g., using Beekeeper).
    user='xxxx', # Add your AWS username here.
    password='xxxxxxx', # Add your AWS server password here.
    host='xxxxxxxx', # Add your AWS server endpoint (address) here.
    port= '5432'
)
cursor = conn.cursor()

issueslist = []
#check to see if the file is a csv
for file in ls:
    if file [-3:] != 'csv':
        print("not a csv")
#if the file is a csv file, check to see if that file has already been ingested.
    elif file[-3:] == 'csv':
        query =""" SELECT COUNT(*)
             FROM ingest
             WHERE filename = %s;"""
        cursor.execute(query,(file,))
        y = cursor.fetchone()
        if y[0] == True:
            print(file, "has already been processed")
        else:
            #if file has not been ingested, open file
           with open(path+'/'+file, mode='r') as csvfile:
               #Skip header
               updated_file = next(csvfile)
               for line in csvfile:
                   issueslist = []
                   #strip rows spaces and split the line into a list
                   row = line.strip().split(',')
                   #check if the row has too many columns
                   if len(row) > 5:
                       issueslist.append("Too Many Columns")
                    #check if the row has too few columns
                   elif len(row) < 5:
                       issueslist.append("Too Few Columns")
                    #check if the first column has less than 1 character
                   if len(row[0]) < 1:
                       issueslist.append("Name Too Short")
                    #check if the first column has more than 100 characters
                   elif len(row[0])>100:
                       issueslist.append("Name Too Long")
                   if len(row[1]) >= 1:
                       if row[1][0].lower() not in ['p','c']:
                           issueslist.append("Breed Issue")
                   elif len(row[1]) < 1:
                       issueslist.append("Breed Issue")
                   if len(row[2]) >= 1:
                       if row[2].lower() not in ['m','f','sf','nm']:
                           issueslist.append("Gender Col Issue")
                   elif len(row[2])<1:
                       issueslist.append("Gender Col Issue")
                   if len(row[3]) >= 1:
                       try:
                           weight = int(row[3])
                       except:
                           issueslist.append("Weight Non-Numeric")
                       if weight < 15:
                              issueslist.append("Weight Too Low")
                       elif weight > 35:
                              issueslist.append("Weight Too High")
                   if len(row[4]) >=1:
                       try:
                           age = float(row[4])
                       except:
                           issueslist.append("Age Non-Numeric")
                       if age < 1:
                           issueslist.append("Age Too Low")
                       elif age > 10:
                           issueslist.append("Age Too High")
                   print(file)
                   print(issueslist)

query= ''' CREATE TABLE IF NOT EXISTS corgi_exception(
            exceptid SERIAL NOT NULL PRIMARY KEY,
            except_record TEXT,
            origin_file VARCHAR(100) NOT NULL,
            issues TEXT,
            except_timestamp TIMESTAMP,
            fixed_timestamp TIMESTAMP);'''
cursor.execute(query,conn)
conn.commit()

cursor = conn.cursor()
#create issues list
issueslist = []
#check to see if the file is a csv
for file in ls:
    if file [-3:] != 'csv':
        print("not a csv")
#if the file is a csv file, check to see if that file has already been ingested.
    elif file[-3:] == 'csv':
        query =""" SELECT COUNT(*)
             FROM ingest
             WHERE filename = %s;"""
        cursor.execute(query,(file,))
        y = cursor.fetchone()
        if y[0] == True:
            print(file, "has already been processed")
        else:
            #if file has not been ingested, open file
           with open(path+'/'+file, mode='r') as csvfile:
               #Skip header
               updated_file = next(csvfile)
               for line in csvfile:
                   issueslist = []
                   ingestlist = []
                   exceptionlist = []
                   #strip rows spaces and split the line into a list
                   row = line.strip().split(',')
                   #check if the row has too many columns
                   if len(row) > 5:
                       issueslist.append("Too Many Columns")
                    #check if the row has too few columns
                   elif len(row) < 5:
                       issueslist.append("Too Few Columns")
                    #check if the first column has less than 1 character
                   if len(row[0]) < 1:
                       issueslist.append("Name Too Short")
                    #check if the first column has more than 100 characters
                   elif len(row[0])>100:
                       issueslist.append("Name Too Long")
                   if len(row[1]) >= 1:
                       if row[1][0].lower() not in ['p','c']:
                           issueslist.append("Breed Issue")
                   elif len(row[1]) < 1:
                       issueslist.append("Breed Issue")
                   if len(row[2]) >= 1:
                       if row[2].lower() not in ['m','f','sf','nm']:
                           issueslist.append("Gender Col Issue")
                   elif len(row[2])<1:
                       issueslist.append("Gender Col Issue")
                   if len(row[3]) >= 1:
                       try:
                           weight = int(row[3])
                       except:
                           issueslist.append("Weight Non-Numeric")
                       if weight < 15:
                              issueslist.append("Weight Too Low")
                       elif weight > 35:
                              issueslist.append("Weight Too High")
                   if len(row[4]) >=1:
                       try:
                           age = float(row[4])
                       except:
                           issueslist.append("Age Non-Numeric")
                       if age < 1:
                           issueslist.append("Age Too Low")
                       elif age > 10:
                           issueslist.append("Age Too High")
                   if len(issueslist) == 0:
                       ingestlist.append(row)
                       df=pd.DataFrame(ingestlist,columns=['corgname','breed','gender','weight','age'])
                       df['breed'].replace(to_replace=["PWC","CWC"],value=["Pem","Cardi"],inplace=True)
                       df['gender'].replace(to_replace="Fem",value="F",inplace=True)
                       df['weight']=df['weight'].round().astype('int')
                       df['age']=np.round(df['age'].astype(float)*2)/2
                       print(df)
                       for x in df.index:
                           query2='''INSERT INTO corgi (corgname,breed,gender,weight,age,fromfile)
                                VALUES('%s','%s','%s',%s,%s,'%s')''' % (df['corgname'].loc[x],df['breed'].loc[x],df['gender'].loc[x],df['weight'].loc[x],df['age'].loc[x],file)
                           cursor.execute(query2)
                           conn.commit()
                           print("Record created for ", df['corgname'].loc[x])
                   else:
                       exceptionlist.append(row)
                       record_value = '|'.join(row)
                       issue_value = '|'.join(issueslist)
                       query3= '''INSERT INTO corgi_exception( except_record,origin_file,issues,except_timestamp)
                               VALUES('%s','%s','%s',current_timestamp)''' % (record_value,file,issue_value)
                       cursor.execute(query3)
                       conn.commit()
                       print("record for ", row[0],"had these issues:", issue_value)
                       query4 = '''INSERT INTO ingest(Filename,Whendone)
                                VALUES('%s',current_timestamp);''' % file
                       cursor.execute(query3)
                       conn.commit()
        print("Ingestion complete for " + file)
cursor = conn.cursor()
query ='''SELECT exceptid,except_record,origin_file
        FROM corgi_exception
        ORDER BY except_record
        LIMIT 10;'''
cursor.execute(query,conn)
df = pd.read_sql(query,conn)
print(df)
cursor = conn.cursor()

doublecheck = '''SELECT C.corgid, C.corgname, C.fromfile, DATE(I.whendone) AS dateingressed
                FROM corgi C JOIN ingest I ON C.fromfile = I.filename
                WHERE I.whendone = CURRENT_DATE
                ORDER BY corgname
                LIMIT 10;'''
cursor.execute(doublecheck)
df = pd.read_sql(doublecheck,conn,index_col='corgid')
print(df)
