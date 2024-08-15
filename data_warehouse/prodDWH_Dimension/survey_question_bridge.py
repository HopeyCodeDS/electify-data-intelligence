import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB

def fill_bridge_tables():
    """Fill the survey_question_bridge table with appropriate survey_id and question_id."""
    try:
        # Load connection configurations
        config_op = load_config_localDB()
        config_dwh = load_config_prodDWH()

        # Establish connections
        conn_op = psycopg2.connect(**config_op)
        conn_dwh = psycopg2.connect(**config_dwh)

        # Create cursors
        cursor_op = conn_op.cursor()
        cursor_dwh = conn_dwh.cursor()

        # Fill survey_question_bridge table
        survey_question_query = """
            SELECT
                q.id AS question_id,
                qnnr.id AS questionnaire_id
            FROM
                question q
            LEFT JOIN
                questionnaire qnnr ON q.questionnaire_id = qnnr.id
            ORDER BY
                q.id, qnnr.id;
            """

        cursor_op.execute(survey_question_query)
        print("About to load data into the survey_question_bridge table...")

        # Create a mapping of survey_id to number_of_questions
        survey_mapping_query = """
            SELECT 
                survey_id, 
                number_of_questions 
            FROM 
                fact_survey;
            """
        cursor_dwh.execute(survey_mapping_query)
        survey_mapping = {row[0]: row[1] for row in cursor_dwh.fetchall()}

        for row in cursor_op.fetchall():
            question_id, questionnaire_id = row

            # Find the corresponding survey_id based on the number of questions
            for survey_id, num_questions in survey_mapping.items():
                if num_questions > 0:  # Only consider surveys with questions
                    # Fetch dim_question data
                    cursor_dwh.execute("SELECT question_id FROM dim_question WHERE question_id = %s", (question_id,))
                    question_id_row = cursor_dwh.fetchone()
                    if question_id_row:
                        question_id = question_id_row[0]
                    else:
                        print(f"Error: question_id not found for question_id: {question_id}")
                        continue

                    # Insert into survey_question_bridge
                    insert_query = """
                        INSERT INTO survey_question_bridge (survey_id, question_id)
                        VALUES (%s, %s)
                        """
                    cursor_dwh.execute(insert_query, (survey_id, question_id))
                    # Decrease the number_of_questions for the survey
                    survey_mapping[survey_id] -= 1
                    break  # Move to the next question after inserting

        conn_dwh.commit()
        print("survey_question_bridge table data filled successfully!")

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
    fill_bridge_tables()
    print()
    print('Loading complete!\n')
