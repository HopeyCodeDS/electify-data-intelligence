import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config, load_config2

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

    # Fetch data from questionnaire
    questionnaireQuery = """
        SELECT questionnaire.id as survey_id, organization_id as city_id FROM questionnaire
        JOIN organization ON organization.id = questionnaire.organization_id
        """
    cursor_op.execute(questionnaireQuery)

    for row in cursor_op.fetchall():
        survey_id, city_id = row

        # Check if the record already exists in the target database
        cursor_dwh.execute("SELECT survey_id FROM survey_city_bridge WHERE survey_id = %s", (survey_id,))
        if not cursor_dwh.fetchone():

            # Fetch fact_survey data
            factQuery = "select survey_id from fact_survey where survey_id = %s"
            cursor_dwh.execute(factQuery, (survey_id,))
            surveyId = cursor_dwh.fetchone()
            if surveyId:
                surveyId = surveyId[0]
            else:
                print(f"Error: survey_id not found for survey_id: {survey_id}")
                continue

            # Fetch dim_City data
            cityQuery = "select city_id from dim_City where city_id = %s"
            cursor_dwh.execute(cityQuery, (city_id,))
            cityId = cursor_dwh.fetchone()
            if cityId:
                cityId = cityId[0]
            else:
                print(f"Error: city_id not found for city_id: {city_id}")
                continue

            # sql query to truncate table
            del_query = """
                        TRUNCATE TABLE survey_city_bridge CASCADE;
                        """
            cursor_dwh.execute(del_query)

            insert_query = """
                            INSERT INTO survey_city_bridge (survey_id, city_id) 
                            VALUES (%s, %s);
                            """
            cursor_dwh.execute(insert_query, (surveyId, cityId))

            # Commit the transaction
            conn_dwh.commit()


except (psycopg2.DatabaseError, Exception) as error:
    print(f"Error: {error}")
