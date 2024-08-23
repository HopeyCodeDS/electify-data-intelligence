import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_test, load_config_localDB


def table_exists(cursor, table_name):
    """Check if a table exists in the PostgreSQL database"""
    exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
    cursor.execute(exists_query, [table_name])
    return cursor.fetchone()[0]


def create_dim_organization_table(cursor):
    """Create the dim_Organization table if it doesn't exist"""
    create_table_query = """
        CREATE TABLE IF NOT EXISTS dim_Organization (
            organization_id SERIAL PRIMARY KEY,
            organization_name VARCHAR(255) NOT NULL
        )
    """
    cursor.execute(create_table_query)
    print("Table 'dim_Organization' created successfully.")


def insert_into_dim_organization(cursor, organization_info):
    """Insert data into the dim_Organization table"""
    insert_query = """
        INSERT INTO dim_Organization (organization_name) 
        VALUES (%s)
    """
    for index, row in organization_info.iterrows():
        cursor.execute(insert_query, (row['name'],))


def main():
    try:
        print("Connection to source database in progress...")
        source_config = load_config_localDB()
        with psycopg2.connect(**source_config) as source_conn:
            with source_conn.cursor() as source_cur:
                # SQL query to retrieve data from the organization table in the source database
                select_query = """
                    SELECT name FROM organization;
                """
                organization_info = pd.read_sql(select_query, source_conn)

        print("Connection to test DWH in progress...")
        dwh_config = load_config_test()
        with psycopg2.connect(**dwh_config) as dwh_conn:
            with dwh_conn.cursor() as dwh_cur:
                if not table_exists(dwh_cur, 'dim_Organization'):
                    create_dim_organization_table(dwh_cur)
                else:
                    print("Table 'dim_Organization' already exists.")

                # Truncate the dim_Organization table before inserting new data
                truncate_query = """
                    TRUNCATE TABLE dim_Organization CASCADE;
                """
                dwh_cur.execute(truncate_query)

                # Insert data into the dim_Organization table
                insert_into_dim_organization(dwh_cur, organization_info)

                # Commit the transaction
                dwh_conn.commit()

        print("ETL for dim_Organization completed successfully.")
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error: {error}")


if __name__ == "__main__":
    main()
