import psycopg2
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDB, load_config_test


def table_exists(cursor, table_name):
    """Check if a table exists in the PostgreSQL database"""
    exists_query = sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)")
    cursor.execute(exists_query, [table_name])
    return cursor.fetchone()[0]


def create_dim_country_table(cursor):
    """Create the dim_Country table if it doesn't exist"""
    create_table_query = """
        CREATE TABLE IF NOT EXISTS dim_Country (
            country_id SERIAL PRIMARY KEY,
            country_name VARCHAR(255) NOT NULL
        )
    """
    cursor.execute(create_table_query)
    print("Table 'dim_Country' created successfully.")


def insert_into_dim_country(cursor):
    """Insert data into the dim_Country table"""
    # Sample data - Replace with actual data source
    country_data = [
        ("Belgium",),
        ("USA",),
        ("China",),
        ("Japan",),
        ("Romania",)
    ]

    insert_query = """
        INSERT INTO dim_Country (country_name)
        VALUES (%s)
    """
    cursor.executemany(insert_query, country_data)


def main():
    try:
        print("Connection to test DWH in progress...")
        config = load_config_test()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                if not table_exists(cur, 'dim_Country'):
                    create_dim_country_table(cur)
                insert_into_dim_country(cur)
        print("ETL test for dim_Country completed successfully.")
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


if __name__ == "__main__":
    main()
