import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB


def fill_Fact_table():
    """Fill the Fact_survey table"""
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

        # Query to fetch data from the operational database
        questionnaire_query = """
        SELECT
            qnnr.id AS questionnaire_id,
            qnnr.datecreated AS qnnr_datecreated,
            org.id AS organization_id,
            t.id AS theme_id,
            st.id AS subtheme_id,
            COUNT(q.id) AS number_of_questions
        FROM
             questionnaire qnnr
        LEFT JOIN
            organization org ON org.id = qnnr.organization_id
        LEFT JOIN
            question q ON q.questionnaire_id = qnnr.id
        LEFT JOIN
            theme t ON q.theme_id = t.id
        LEFT JOIN
            subtheme st ON q.subtheme_id = st.id
        GROUP BY
            qnnr.id,
            qnnr.datecreated,
            org.id,
            t.id,
            st.id
        ORDER BY
            qnnr.id,org.id,t.id,st.id;
    """

        cursor_op.execute(questionnaire_query)

        print("About to load data into the fact_survey table...")

        for row in cursor_op.fetchall():
            questionnaire_id, qnnr_datecreated, organization_id, theme_id, subtheme_id, number_of_questions = row

            # Add logging to inspect values
            print(f"Processing questionnaire_id: {questionnaire_id}, qnnr_datecreated: {qnnr_datecreated}")

            # Fetch dim_Date
            cursor_dwh.execute("SELECT date_id FROM dim_Date WHERE date = %s", (qnnr_datecreated,))
            date_id = cursor_dwh.fetchone()
            if date_id:
                date_id = date_id[0]
            else:
                print(f"Error: date_id not found for date: {qnnr_datecreated}")
                continue

            # Fetch dim_Theme data
            cursor_dwh.execute("SELECT theme_id FROM dim_Theme WHERE theme_id = %s", (theme_id,))
            theme_id_row = cursor_dwh.fetchone()
            if theme_id_row:
                theme_id = theme_id_row[0]
            else:
                print(f"Error: theme_id not found for theme_id: {theme_id}")
                continue

            # Fetch dim_SubTheme data
            cursor_dwh.execute("SELECT subtheme_id FROM dim_SubTheme WHERE subtheme_id = %s", (subtheme_id,))
            subtheme_id_row = cursor_dwh.fetchone()
            if subtheme_id_row:
                subtheme_id = subtheme_id_row[0]
            else:
                print(f"Error: subtheme_id not found for subtheme_id: {subtheme_id}")
                continue

            # Fetch dim_Organization data
            cursor_dwh.execute("SELECT organization_id FROM dim_Organization WHERE organization_id = %s", (organization_id,))
            organization_id_row = cursor_dwh.fetchone()
            if organization_id_row:
                organization_id = organization_id_row[0]
            else:
                print(f"Error: organization_id not found for organization_id: {organization_id}")
                continue

            # Insert into Fact_survey
            insert_query = """
            INSERT INTO fact_survey (date_id, theme_id, subtheme_id, organization_id, number_of_questions)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor_dwh.execute(insert_query, (date_id, theme_id, subtheme_id, organization_id, number_of_questions))

            conn_dwh.commit()
            print("Fact table data filled successfully!")

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
    fill_Fact_table()
    print()
    print('Loading in complete!\n')