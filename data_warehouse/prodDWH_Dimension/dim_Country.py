import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB

try:
    print("Connection to ElectifyDB in progress...")
    con = load_config_localDB()
    print('Loading ElectifyDB in progress...\n')
    with psycopg2.connect(**con) as conn:
        with conn.cursor() as cur:
            # sql query to retrieve data from the organization table in the operational database
            sql = """
                select country from organization;
               """
    countryInfo = pd.read_sql(sql, conn)

    print("Connection to DWH in progress...")
    configure = load_config_prodDWH()
    print('Loading in progress...\n')
    with psycopg2.connect(**configure) as conn:
        with conn.cursor() as cur:
            # sql query to truncate table
            del_query = """
            TRUNCATE TABLE dim_Country cascade;
            """
            cur.execute(del_query)

            # sql query to insert
            insert_query = """
            INSERT INTO dim_Country (country_name) 
            VALUES (%s);
            """

            # Execute the INSERT query
            for index, row in countryInfo.iterrows():
                cur.execute(insert_query, (row['country'],))

            # Commit the transaction
            conn.commit()

except (psycopg2.DatabaseError, Exception) as error:
    print(f"Error: {error}")
