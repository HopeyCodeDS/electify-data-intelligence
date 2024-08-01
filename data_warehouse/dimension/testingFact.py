import psycopg2
from data_warehouse.database_connection.config import load_config, load_config2
from datetime import datetime

def get_last_load_time(cursor, table_name):
    cursor.execute("SELECT last_load_time FROM etl_tracking WHERE table_name = %s", (table_name,))
    result = cursor.fetchone()
    return result[0] if result else None

def update_last_load_time(cursor, table_name, load_time):
    cursor.execute("""
        INSERT INTO etl_tracking (table_name, last_load_time)
        VALUES (%s, %s)
        ON CONFLICT (table_name) DO UPDATE SET last_load_time = EXCLUDED.last_load_time;
    """, (table_name, load_time))

def fill_Fact_table():
    try:
        # Load connection configurations
        config_op = load_config2()
        config_dwh = load_config()

        # Establish connections
        conn_op = psycopg2.connect(**config_op)
        conn_dwh = psycopg2.connect(**config_dwh)

        # Create cursors
        cursor_op = conn_op.cursor()
        cursor_dwh = conn_dwh.cursor()

        # Specify the source table name
        source_table_name = 'questionnaire'  # Change to the appropriate source table name

        # Fetch the last load time for the source table
        last_load_time = get_last_load_time(cursor_dwh, source_table_name)

        # Fetch new and updated records from the source table
        if last_load_time:
            query = f"""
            SELECT
                -- Select your columns here
            FROM
                {source_table_name}
            WHERE
                updated_at > %s;  -- Compare with the last load time
            """
            cursor_op.execute(query, (last_load_time,))
        else:
            query = f"SELECT * FROM {source_table_name};"
            cursor_op.execute(query)

        # Loop through the fetched records and insert into the data warehouse fact table
        for row in cursor_op.fetchall():
            # Insert logic for fetching and inserting records into the fact table
            pass

        # Update the last load time in the data warehouse
        update_last_load_time(cursor_dwh, source_table_name, datetime.now())
        conn_dwh.commit()

    except Exception as error:
        print("Error while working with PostgreSQL:", error)

    finally:
        # Close cursors and connections
        if 'cursor_op' in locals():
            cursor_op.close()
        if 'cursor_dwh' in locals():
            cursor_dwh.close()
        if 'conn_op' in locals():
            conn_op.close()
        if 'conn_dwh' in locals():
            conn_dwh.close()

if __name__ == "__main__":
    fill_Fact_table()
    print()
    print('Loading is complete!\n')
