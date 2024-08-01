import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config, load_config2, load_config_gcloud

try:
    print("Connection to ElectifyDB in progress...")
    con = load_config2()
    print('Loading ElectifyDB in progress...\n')
    with psycopg2.connect(**con) as conn:
        with conn.cursor() as cur:
            # sql query to retrieve data from the organization table in the operational database
            sql = """
                            select name from organization;
                           """
            organizationInfo = pd.read_sql(sql, conn)
            print(organizationInfo)

    print("Connection to DWH in progress...")
    configure = load_config_gcloud()
    print('Loading in progress...\n')
    with psycopg2.connect(**configure) as conn:
        with conn.cursor() as cur:
            # sql query to truncate table
            del_query = """
            TRUNCATE TABLE dim_Organization cascade;
            """
            cur.execute(del_query)

            # sql query to insert
            insert_query = """
            INSERT INTO dim_Organization (organization_name)
            VALUES (%s);
            """

            # Execute the INSERT query
            for index, row in organizationInfo.iterrows():
                cur.execute(insert_query, (row['name'],))

            # Commit the transaction
            conn.commit()

except (psycopg2.DatabaseError, Exception) as error:
    print(f"Error: {error}")

