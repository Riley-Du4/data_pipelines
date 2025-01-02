import pandas as pd
import psycopg2

#establishing the connection
conn = psycopg2.connect(
    database="xxx", 
    user='xxx', 
    password='xxx', 
    host='xxxx', 
    port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#b and c
query = '''SELECT SUM(o.lineqty) as num_trailer, o.builtdate - o.promisedate as days_delay, AVG(so.Delsatisfac) as avgcsat
FROM oline as o JOIN SalesOrder as so ON o.orderid = so.orderid
WHERE o.builtdate - o.promisedate > 0
GROUP BY days_delay
ORDER BY days_delay ASC;'''
cursor.execute(query)
df = pd.read_sql(query,conn)
print(df)
print(df.corr())
print("The longer of a delay there is from the promised delivery day to the day it was built, the less satisfied the customer is.")
query2="""
WITH
    UpdatedSalesperson AS (
        SELECT sp.spid,sp.spfname,sp.splname,s.sitename
        FROM salesperson as sp
        JOIN site as s ON sp.siteid = s.siteid
        WHERE sp.termDate IS NULL
    ),
    Trailers as (
        SELECT us.spid,us.spfname,us.splname,us.sitename,ol.orderid, COUNT(*) as trailercnt
        FROM UpdatedSalesperson as us JOIN salesorder as so ON us.spid = so.sellerid
        JOIN oline as ol ON so.orderid = ol.orderid
        JOIN product as p ON ol.productid = p.prodid
        JOIN prodcat as pc ON p.catid = pc.catid
        WHERE pc.category = 'Trailers'
        GROUP BY us.SPID, us.SPFname, us.SPLname, us.SiteName, ol.OrderID
    ),
    SalesInfo as (
        SELECT t.SPID,t.SPFname,t.SPLname,t.SiteName,COUNT(DISTINCT t.OrderID) as trailercnt,
            COUNT(CASE WHEN pc2.Category = 'Upgrades' THEN ol2.OLID END) as upgradescnt,
            COALESCE(SUM(CASE WHEN pc2.Category ='Upgrades' THEN ol2.LineTotal ELSE 0 END), 0) as upgrade_rev
        FROM Trailers as t
        LEFT JOIN OLine as ol2 ON t.OrderID = ol2.OrderID
        LEFT JOIN Product as p2 ON ol2.ProductID = p2.ProdID
        LEFT JOIN ProdCat as pc2 ON p2.CatID = pc2.CatID
        GROUP BY t.SPID,t.SPFname,t.SPLname,t.SiteName
    )
SELECT SPFname as FirstName,SPLname as LastName,SiteName as Location, trailercnt,upgradescnt, upgrade_rev,
       ROUND((upgrade_rev::numeric / trailercnt), 2) as avg_upsell_per_trailer
FROM SalesInfo
ORDER BY avg_upsell_per_trailer DESC;
"""

df2 = pd.read_sql(query2,conn)
print(df2)
import pymssql

conn = pymssql.connect(
     host='xxxx', # Server name goes in quotes.
     user='xxxx', # Username goes in quotes.
     password='xxxx', # Password goes in quotes
     database='xxxx') # Database to use goes in quotes

cursor = conn.cursor()
tables=['custOrder','Order_Line','Inventory']
for x in tables:
    print(x)
    query = """SELECT column_name,data_type,character_maximum_length,numeric_precision, is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';""" % x
    df = pd.read_sql(query,conn,index_col='column_name')
    df.rename(columns={'character_maximum_length':'MaxChar'}, inplace =True)
    print(df)
cursor = conn.cursor()
cursor.execute(''' ALTER TABLE SalesOrder
            ADD COLUMN IF NOT EXISTS TxID INT,
            ADD COLUMN IF NOT EXISTS Tx_IngestTimestamp TIMESTAMP;''')
conn.commit()

query2 = ''' SELECT OrderID,ODate,OTotal,TxID,Tx_IngestTimestamp
FROM SalesOrder
Limit 10;'''
df = pd.read_sql(query2,conn)
print(df)
# create sequence for customer.custid
cursor.execute("""
CREATE SEQUENCE IF NOT EXISTS custid_seq
    OWNED BY customer.custid;
SELECT SETVAL('custid_seq',
                (SELECT max(custid) + 3 FROM customer),FALSE);
ALTER TABLE customer
    ALTER COLUMN custid SET DEFAULT nextval('custid_seq');""")
conn.commit()

#create sequence for salesorder.orderid

cursor.execute("""
CREATE SEQUENCE IF NOT EXISTS orderid_seq
    OWNED BY salesorder.orderid;
SELECT SETVAL('orderid_seq',(SELECT max(orderid) + 3 FROM salesorder),FALSE);
ALTER TABLE salesorder
    ALTER COLUMN orderid SET DEFAULT nextval('orderid_seq');""")
conn.commit()
#ingressing Salt Lake data
import pymssql
salt_conn = pymssql.connect(
     host='xxxx', # Server name goes in quotes.
     user='xxxxx', # Username goes in quotes.
     password='xxxxx', # Password goes in quotes
     database='xxxxx',
     port='xxxxx') # Database to use goes in quotes
central_conn = psycopg2.connect(
      database="xxxxx", 
    user='xxxxx', 
    password='xxxxx', 
    host='xxxxx', 
    port= 'xxxx')
    
central_cursor = central_conn.cursor()

#create dataframe for customerorder table
df = pd.read_sql("""SELECT *
    FROM custorder""", salt_conn)

#insert values into dataframe
for info in df.index:
    site = 'SL'
    custname = df['custname'].loc[info]
    fname, lname =custname.split(' ')
    custphone = df['custphone'].loc[info]
    custemail = df['custemail'].loc[info]
    contactpref = df['contactpref'].loc[info]
    custstate = df['custstate'].loc[info]
    pmtid =df['pmtid'].loc[info]
    
#check for duplicates, and populate new values.
    central_cursor.execute(f'''SELECT COUNT(*) 
                        FROM Customer
                        WHERE custfname = '{fname}' AND custlname = '{lname}'
                        ''')
    result = central_cursor.fetchone()
    if result[0] > 1:
        print('Name', fname, lname)
    else:
        central_cursor.execute('''
        INSERT INTO Customer (custfname, custlname, custphone, custemail, custstate, contactpref, pmtid, addedstamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)''', (fname, lname, custphone, custemail, custstate, contactpref, pmtid))
        central_conn.commit()
#get the customerid
    central_cursor.execute(f'''SELECT custid FROM Customer WHERE custfname = '{fname}' AND custlname = '{lname}'
    ''')
    cust_id = central_cursor.fetchone()
    custid = cust_id[0]
#ingress salesperson
    salesperson = df['salesperson'].loc[info]
    sales_fname, sales_lname = salesperson.split(' ')
#spid
    central_cursor.execute(f''' SELECT SPID FROM SalesPerson WHERE spfname = '{sales_fname}' AND splname = '{sales_lname}'
    ''')
    y = central_cursor.fetchone()
    spid = y[0]


#ingress Order details
    orderid = df['orderid'].loc[info]
    Odate = df['odate'].loc[info]
    Ototal = df['saletotal'].loc[info]
#Insert Sales ORder
    central_cursor.execute(f'''
    INSERT INtO SalesOrder (Custid, sellerid, odate, ototal, txid, tx_ingesttimestamp)
    VALUES('{custid}', '{spid}', '{Odate}', '{Ototal}', '{orderid}', CURRENT_TIMESTAMP)''')
    central_conn.commit()
#updated orderid
    central_cursor.execute(f''' SELECT Orderid FROM SalesOrder WHERE txid = '{orderid}'
    ''')
    r =  central_cursor.fetchone()
    cr_orderid = r[0]

    promisedate = df['promisedate'].loc[info]
    oline = f'''SELECT * FROM Order_line WHERE OrderID ={orderid}'''
    oldf= pd.read_sql(oline, salt_conn)
    
    for line in oldf.index:
        prodname = oldf['prodname'].loc[line]
        lineqty = oldf['qty'].loc[line]
        linetotal = oldf['linetotal'].loc[line]
        lineprice = linetotal/lineqty

        central_cursor.execute(f''' SELECT ProdID FROM Product WHERE Prodname = '{prodname}'
        ''')
        productid = central_cursor.fetchone()[0]
        #ingress oline
        central_cursor.execute(f'''
        INSERT INTO Oline (OrderID, productID, lineqty, lineprice, linetotal, promisedate)
        VALUES('{cr_orderid}','{productid}','{lineqty}','{lineprice}','{linetotal}',%s)''',(promisedate,))
        central_conn.commit()
    print(f"Order {orderid} from {site} ingested as OrderID {cr_orderid}")


#Ingression from Boise database
boise_conn = pymssql.connect(
    server='xxxx',
    user='xxxxx',
    password='xxxxx',
    database='xxxxx',
    port='xxxxx'
)


df = pd.read_sql('''
                    SELECT * 
                    FROM CustOrder
                    ''', boise_conn)

for info in df.index:
    site = 'Boise'
    custname = df['custname'].loc[info]
    fname, lname =custname.split(' ')
    custphone = df['custphone'].loc[info]
    custemail = df['custemail'].loc[info]
    contactpref = df['contactpref'].loc[info]
    custstate = df['custstate'].loc[info]
    pmtid =df['pmtid'].loc[info]
       
#check for duplicates, and populate new values.
    central_cursor.execute(f'''SELECT COUNT(*) 
                        FROM Customer
                        WHERE custfname = '{fname}' AND custlname = '{lname}'
                        ''')
    result = central_cursor.fetchone()
    if result[0] > 1:
        print('Name', fname, lname)
    else:
        central_cursor.execute('''
        INSERT INTO Customer (custfname, custlname, custphone, custemail, custstate, contactpref, pmtid, addedstamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)''', (fname, lname, custphone, custemail, custstate, contactpref, pmtid))
        central_conn.commit()
#get the customerid
    central_cursor.execute(f'''SELECT custid FROM Customer WHERE custfname = '{fname}' AND custlname = '{lname}'
    ''')
    cust_id = central_cursor.fetchone()
    custid = cust_id[0]
#ingress salesperson
    salesperson = df['salesperson'].loc[info]
    sales_fname, sales_lname = salesperson.split(' ')
#spid
    central_cursor.execute(f''' SELECT SPID FROM SalesPerson WHERE spfname = '{sales_fname}' AND splname = '{sales_lname}'
    ''')
    y = central_cursor.fetchone()
    spid = y[0]


#ingress Order details
    orderid = df['orderid'].loc[info]
    Odate = df['odate'].loc[info]
    Ototal = df['saletotal'].loc[info]
#Insert Sales ORder
    central_cursor.execute(f'''
    INSERT INtO SalesOrder (Custid, sellerid, odate, ototal, txid, tx_ingesttimestamp)
    VALUES('{custid}', '{spid}', '{Odate}', '{Ototal}', '{orderid}', CURRENT_TIMESTAMP)''')
    central_conn.commit()
#updated orderid
    central_cursor.execute(f''' SELECT Orderid FROM SalesOrder WHERE txid = '{orderid}'
    ''')
    r =  central_cursor.fetchone()
    cr_orderid = r[0]

    promisedate = df['promisedate'].loc[info]
    oline = f'''SELECT * FROM Order_line WHERE OrderID ={orderid}'''
    oldf= pd.read_sql(oline, boise_conn)
    
    for line in oldf.index:
        prodname = oldf['prodname'].loc[line]
        lineqty = oldf['qty'].loc[line]
        linetotal = oldf['linetotal'].loc[line]
        lineprice = linetotal/lineqty

        central_cursor.execute(f''' SELECT ProdID FROM Product WHERE Prodname = '{prodname}'
        ''')
        productid = central_cursor.fetchone()[0]
        #ingress oline
        central_cursor.execute(f'''
        INSERT INTO Oline (OrderID, productID, lineqty, lineprice, linetotal, promisedate)
        VALUES('{cr_orderid}','{productid}','{lineqty}','{lineprice}','{linetotal}',%s)''',(promisedate,))
        central_conn.commit()
    print(f"Order {orderid} from {site} ingested as OrderID {cr_orderid}")

#ingestion for Phoenix
df = pd.read_sql('''
                    SELECT * 
                    FROM CustOrder
                    ''', phoenix_conn)

for info in df.index:
    site = 'Phoenix'
    custname = df['custname'].loc[info]
    fname, lname =custname.split(' ')
    custphone = df['custphone'].loc[info]
    custemail = df['custemail'].loc[info]
    contactpref = df['contactpref'].loc[info]
    custstate = df['custstate'].loc[info]
    pmtid =df['pmtid'].loc[info]
       
#check for duplicates, and populate new values.
    central_cursor.execute(f'''SELECT COUNT(*) 
                        FROM Customer
                        WHERE custfname = '{fname}' AND custlname = '{lname}'
                        ''')
    result = central_cursor.fetchone()
    if result[0] > 1:
        print('Name', fname, lname)
    else:
        central_cursor.execute('''
        INSERT INTO Customer (custfname, custlname, custphone, custemail, custstate, contactpref, pmtid, addedstamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)''', (fname, lname, custphone, custemail, custstate, contactpref, pmtid))
        central_conn.commit()
#get the customerid
    central_cursor.execute(f'''SELECT custid FROM Customer WHERE custfname = '{fname}' AND custlname = '{lname}'
    ''')
    cust_id = central_cursor.fetchone()
    custid = cust_id[0]
#ingress salesperson
    salesperson = df['salesperson'].loc[info]
    sales_fname, sales_lname = salesperson.split(' ')
#spid
    central_cursor.execute(f''' SELECT SPID FROM SalesPerson WHERE spfname = '{sales_fname}' AND splname = '{sales_lname}'
    ''')
    y = central_cursor.fetchone()
    spid = y[0]


#ingress Order details
    orderid = df['orderid'].loc[info]
    Odate = df['odate'].loc[info]
    Ototal = df['saletotal'].loc[info]
#Insert Sales ORder
    central_cursor.execute(f'''
    INSERT INtO SalesOrder (Custid, sellerid, odate, ototal, txid, tx_ingesttimestamp)
    VALUES('{custid}', '{spid}', '{Odate}', '{Ototal}', '{orderid}', CURRENT_TIMESTAMP)''')
    central_conn.commit()
#updated orderid
    central_cursor.execute(f''' SELECT Orderid FROM SalesOrder WHERE txid = '{orderid}'
    ''')
    r =  central_cursor.fetchone()
    cr_orderid = r[0]

    promisedate = df['promisedate'].loc[info]
    oline = f'''SELECT * FROM Order_line WHERE OrderID ={orderid}'''
    oldf= pd.read_sql(oline, phoenix_conn)
    
    for line in oldf.index:
        prodname = oldf['prodname'].loc[line]
        lineqty = oldf['qty'].loc[line]
        linetotal = oldf['linetotal'].loc[line]
        lineprice = linetotal/lineqty

        central_cursor.execute(f''' SELECT ProdID FROM Product WHERE Prodname = '{prodname}'
        ''')
        productid = central_cursor.fetchone()[0]
        #ingress oline
        central_cursor.execute(f'''
        INSERT INTO Oline (OrderID, productID, lineqty, lineprice, linetotal, promisedate)
        VALUES('{cr_orderid}','{productid}','{lineqty}','{lineprice}','{linetotal}',%s)''',(promisedate,))
        central_conn.commit()
    print(f"Order {orderid} from {site} ingested as OrderID {cr_orderid}")
  
query = """
SELECT ol.orderid, so.odate, SUM(ol.lineqty) as total_qty, so.ototal,sp.siteid,c.custlname
FROM oline as ol JOIN salesorder as so ON ol.orderid = so.orderid
JOIN customer as c ON so.custid =  c.custid
JOIN salesperson as sp ON so.sellerid = sp.spid
WHERE DATE(so.tx_ingesttimestamp) = CURRENT_DATE
GROUP BY ol.orderid, so.odate,so.ototal,sp.siteid,c.custlname
ORDER BY so.odate, c.custlname;
"""
df= pd.read_sql_query(query, conn, index_col='orderid')
print(df)
