import pyodbc as py
import pandas as pd
import datetime
import tkinter as tk




def sql_alternate_id(account_id,identifiers,recon_date):
        # Set up the database connection parameters
        server = 'P01-01-AG-004'
        database = 'custodydata'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207&'
        driver = '{ODBC Driver 17 for SQL Server}'
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''
        SELECT DISTINCT 
        CASE WHEN AlternateId IS NULL THEN '*' ELSE AlternateId END AS AlternateId,
        CASE WHEN Cusip IS NULL THEN '*' ELSE Cusip END AS Cusip,
        CASE WHEN Isin IS NULL THEN '*' ELSE Isin END AS Isin,
        CASE WHEN Sedol IS NULL THEN '*' ELSE Sedol END AS Sedol,
        CASE WHEN Ticker IS NULL THEN '*' ELSE Ticker END AS Ticker,
        CASE WHEN MaturityDate IS NULL THEN '*' ELSE MaturityDate END AS MaturityDate
        
        
        FROM data.prometheuspricing
        WHERE 1=1
        AND NormalizationModelId = 2397
        AND Recondate = '{recon_date}'
        AND partitionid = {account_id}-- simple sleeve account
        AND Alternateid IN ({identifiers})
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df, print("SQL executed within the timeout period")


def sql_sec_mappings(sleeve_agg,identifiers):
        # Set up the database connection parameters
        server = 'PROD-SQL-RO'
        database = 'LM'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207&'
        driver = '{ODBC Driver 17 for SQL Server}'
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''
        SELECT   sm.Cusip 'SMCusip', so.Cusip [Custody Cusip],  so.AlternateId, so.MaturityDate, so.ISIN, so.sedol, so.Ticker, so.SecurityID
        FROM [cerberus].[SecurityOverrides] so
        JOIN dbo.SecurityMaster sm ON sm.Id = so.SecurityId
        JOIN dbo.Accounts a ON a.id = so.PartitionId
        JOIN Clients c ON c.id = dbo.findUltimateNonCustodianParentClient(a.clientid)
        WHERE 1=1
        AND so.PartitionId = {sleeve_agg} --accountid
        AND sm.Cusip IN ({identifiers})
        AND c.id = 25376 --ultimate parent clientid
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df, print("SQL executed within the timeout period")


def sql_cusip_id(account_id,identifiers,recon_date):
        # Set up the database connection parameters
        server = 'P01-01-AG-004'
        database = 'custodydata'
        username = 'ARBFUND\matthewray'
        password = 'Uhglbk547895207&'
        driver = '{ODBC Driver 17 for SQL Server}'
        

        # Create the connection string
        conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Trusted_Connection=yes;TrustServerCertificate=yes;MultiSubnetFailover=yes'

        # Connect to the database
        conn = py.connect(conn_str)

        # Create a cursor object to execute the SQL statements
        cursor = conn.cursor()

        cursor.execute('SET QUERY_GOVERNOR_COST_LIMIT 300')

        # Execute a SQL query
        query = f'''
        SELECT DISTINCT 
        CASE WHEN AlternateId IS NULL THEN '*' ELSE AlternateId END AS AlternateId,
        CASE WHEN Cusip IS NULL THEN '*' ELSE Cusip END AS Cusip,
        CASE WHEN Isin IS NULL THEN '*' ELSE Isin END AS Isin,
        CASE WHEN Sedol IS NULL THEN '*' ELSE Sedol END AS Sedol,
        CASE WHEN Ticker IS NULL THEN '*' ELSE Ticker END AS Ticker,
        CASE WHEN MaturityDate IS NULL THEN '*' ELSE MaturityDate END AS MaturityDate
        
        
        FROM data.prometheuspricing
        WHERE 1=1
        AND NormalizationModelId = 2397
        AND Recondate = '{recon_date}'
        AND partitionid = {account_id}-- simple sleeve account
        AND Cusip IN ({identifiers})
        '''

        cursor.execute(query)

        # Fetch all the rows from the query result
        cursor.fetchall()
        df = pd.read_sql(query,conn)

        # Close the cursor and the connection
        cursor.close()
        conn.close()
  

        return df, print("SQL executed within the timeout period")
    
