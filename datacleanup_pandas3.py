#2 Connect to database
import pymssql
import pandas as pd
import numpy as mp
conn = pymssql.connect(
     host='xxxx.xxx.xxx',
     user='xxxxx',
     password='xxxxx',
     database='xxxxx')
cursor = conn.cursor()
query = """
WITH cteHouseHold 
AS (
SELECT
    H.HHID, 
    H.Region, 
    H.HHI, 
    G.Lname, 
    COUNT(DISTINCT G.GuestID) AS Members,
    COUNT(DISTINCT CASE WHEN G.SeasonPass = 'y' THEN G.GuestID END) AS SPCount,
    COUNT(DISTINCT CASE WHEN G.Age < 18 THEN G.GuestID END) AS U18Count, 
    COUNT(V.VisID) AS VisitCt
FROM Household as H 
LEFT JOIN Guest as G
    ON H.HHID = G.HHID
LEFT JOIN Visit as V
    ON G.GuestID = V.CustID
GROUP BY H.HHID, H.Region, H.HHI, G.Lname
)
SELECT * 
FROM cteHouseHold;
"""
df = pd.read_sql(query, conn, index_col ='HHID')
print(df)
#3 Replace nulls (where appropriate)
df['SPCount'].replace(to_replace=np.nan,value=0,inplace=True)
df['U18Count'].replace(to_replace=np.nan,value=0,inplace=True)
df['VisitCt'].replace(to_replace=np.nan,value=0,inplace=True)
print(df)
#4 New Calculated Column
df['VisPP']=df['VisitCt']/df['Members']
print(df[['Lname','Members','VisitCt','VisPP']])
#5 New Conditional Column
df['IsLocal'] = np.where(
    df['Region'] == 'Central PA',
    1,0)
print(df[['Lname','Region','IsLocal']])
#6 Create a Conditional Dataframe Copy
dflocal = df[df['IsLocal'] == 1]
print(dflocal)
#7 Delete a Column
dflocal.drop(columns=['IsLocal','Region'],inplace = True)
print(dflocal)
#8 Create a new column in dflocal
dflocal.dropna(inplace = True)
conditions = [dflocal['Income'] < 50000,
              dflocal['Income'] < 100000,
              dflocal['Income'] < 150000,
              True]
values = ['Under $50K','$50-$100K','$100-$150K','Over $150K']
dflocal['IncomeCat'] = np.select(conditions, values)
print(dflocal[['Lname','Income','IncomeCat','VisPP']])
#9 Small analysis using Aggregation
print(dflocal[['IncomeCat','VisPP']].groupby('IncomeCat').describe())
print(""" The average amount of visits per person increases as Income increases. This is
found from seeing the average and std dev of each income bracket""")
print(dflocal[['Income','Members','VisPP','SPCount','U18Count']].corr())
