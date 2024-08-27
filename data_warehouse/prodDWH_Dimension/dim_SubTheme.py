import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB


try:
    print("Connection to ElectifyDB in progress...")
    # con = load_config_localDB()
    con = load_config_prodDB()
    print('Loading ElectifyDB in progress...\n')
    with psycopg2.connect(**con) as conn:
        with conn.cursor() as cur:
            # sql query to retrieve data from the subtheme table in the operational database
            sql = """
                select subtheme.name from subtheme
                left join theme on subtheme.theme_id = theme.id;
               """
    subthemeInfo = pd.read_sql(sql, conn)

    print("Connection to DWH in progress...")
    configure = load_config_prodDWH()
    print('Loading in progress...\n')
    with psycopg2.connect(**configure) as conn:
        with conn.cursor() as cur:
            # sql query to truncate table
            del_query = """
            TRUNCATE TABLE dim_SubTheme cascade;
            """
            cur.execute(del_query)

            # sql query to insert
            insert_query = """
            INSERT INTO dim_SubTheme (subtheme_name) 
            VALUES (%s);
            """

            # Execute the INSERT query
            for index, row in subthemeInfo.iterrows():
                cur.execute(insert_query, (row['name'],))

            # Commit the transaction
            conn.commit()
            print("Subtheme data inserted successfully!")

except (psycopg2.DatabaseError, Exception) as error:
    print(f"Error: {error}")
