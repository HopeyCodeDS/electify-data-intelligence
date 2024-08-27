import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB


def fill_country_bridge_tables():
    """Fill the survey_country_bridge table with survey_id and organization_id as country_id."""
    try:
        # Load connection configurations
        config_op = load_config_prodDB()
        config_dwh = load_config_prodDWH()

        # Establish connections
        conn_op = psycopg2.connect(**config_op)
        conn_dwh = psycopg2.connect(**config_dwh)

        # Create cursors
        cursor_op = conn_op.cursor()
        cursor_dwh = conn_dwh.cursor()

        # Fetch survey data
        survey_query = """
            SELECT 
                survey_id, 
                organization_id 
            FROM 
                fact_survey;
            """
        cursor_dwh.execute(survey_query)
        survey_data = cursor_dwh.fetchall()

        print("About to load data into the survey_country_bridge table...")

        # Insert into survey_country_bridge
        for survey_id, organization_id in survey_data:
            # Insert into survey_country_bridge
            insert_query = """
                INSERT INTO survey_country_bridge (survey_id, country_id)
                VALUES (%s, %s)
                """
            cursor_dwh.execute(insert_query, (survey_id, organization_id))

        conn_dwh.commit()
        print("survey_country_bridge table data filled successfully!")

    except (Exception, psycopg2.Error) as error:
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
    fill_country_bridge_tables()
    print()
    print('Loading complete!\n')
