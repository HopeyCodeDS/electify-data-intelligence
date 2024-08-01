import psycopg2
from data_warehouse.database_connection.config import load_config, load_config2


def fill_Fact_table():
    """Fill the Fact_survey table"""
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

        # Query to fetch data from the operational database
        questionnaire_query = """
        SELECT
            q.id AS question_id,
            qnnr.theme_id AS theme_id,
            q.creation_date AS question_creation_date,
            org.id AS organization_id,
            COUNT(ans.answer_type) AS number_of_questions_answered,
            org.name AS organization_name,
            org.country AS country,
            org.datecreated AS org_datecreated,
            org.datemodified AS org_datemodified,
            org.status AS org_status,
            qnnr.name AS questionnaire_name,
            qnnr.datecreated AS qnnr_datecreated,
            qnnr.datemodified AS qnnr_datemodified,
            qnnr.type AS questionnaire_type,
            qnnr.status AS questionnaire_status,
            q.question_content,
            q.question_type,
            ans.id AS answer_id,
            ans.answer_type AS answer_type,
            ans.date AS answer_date
        FROM
            organization org
        LEFT JOIN
            questionnaire qnnr ON org.id = qnnr.organization_id
        LEFT JOIN
            question q ON qnnr.id = q.questionnaire_id
        LEFT JOIN
            answer ans ON q.id = ans.question_id
        GROUP BY
            q.id,
            qnnr.theme_id,
            q.creation_date,
            org.id,
            org.name,
            org.country,
            org.datecreated,
            org.datemodified,
            org.status,
            qnnr.name,
            qnnr.datecreated,
            qnnr.datemodified,
            qnnr.type,
            qnnr.status,
            q.question_content,
            q.question_type,
            ans.id,
            ans.answer_type,
            ans.date
        ORDER BY
            org.id, q.id;
    """

        cursor_op.execute(questionnaire_query)

        for row in cursor_op.fetchall():
            question_id, theme_id, question_creation_date, organization_id, number_of_questions_answered, organization_name, country, org_datecreated, org_datemodified, org_status, questionnaire_name, qnnr_datecreated, qnnr_datemodified, questionnaire_type, questionnaire_status, question_content, question_type, answer_id,answer_type, answer_date = row

            # Fetch dim_Date
            cursor_dwh.execute("SELECT date_id FROM dim_Date WHERE date = %s", (question_creation_date,))
            date_id = cursor_dwh.fetchone()
            if date_id:
                date_id = date_id[0]
            else:
                print(f"Error: date_id not found for date: {question_creation_date}")
                continue

            # Fetch dim_Theme data
            cursor_dwh.execute("SELECT theme_id FROM dim_Theme WHERE theme_id = %s", (theme_id,))
            theme_id_row = cursor_dwh.fetchone()
            if theme_id_row:
                theme_id = theme_id_row[0]
            else:
                print(f"Error: theme_id not found for theme_id: {theme_id}")
                continue

            # Fetch dim_Question data
            cursor_dwh.execute("SELECT question_id FROM dim_Question WHERE question_id = %s", (question_id,))
            question_id_row = cursor_dwh.fetchone()
            if question_id_row:
                question_id = question_id_row[0]
            else:
                print(f"Error: question_id not found for question_id: {question_id}")
                continue

            # Fetch dim_Organization data
            cursor_dwh.execute("SELECT organization_id FROM dim_Organization WHERE organization_id = %s",
                               (organization_id,))
            organization_id_row = cursor_dwh.fetchone()
            if organization_id_row:
                organization_id = organization_id_row[0]
            else:
                print(f"Error: organization_id not found for organization_id: {organization_id}")
                continue

                # Fetch dim_Answer data
                cursor_dwh.execute("SELECT answer_id FROM dim_Answer WHERE answer_id = %s", (answer_id,))
                answer_id_row = cursor_dwh.fetchone()
                if answer_id_row:
                    answer_id = answer_id_row[0]
                else:
                    print(f"Error: answer_id not found for answer_id: {answer_id}")
                    continue

            # Fetch dim_Country data
            cursor_dwh.execute("SELECT country_id FROM dim_Country WHERE country_name = %s", (country,))
            country_id_row = cursor_dwh.fetchone()
            if country_id_row:
                country_id = country_id_row[0]
            else:
                print(f"Error: country_id not found for country: {country}")
                continue

            # Insert into Fact_survey
            insert_query = """
        INSERT INTO fact_survey (date_id, theme_id, organization_id, question_id, answer_id, country_id, number_of_questions_answered)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
            cursor_dwh.execute(insert_query,
                               (date_id, theme_id, organization_id, question_id, answer_id, country_id,
                                number_of_questions_answered))

            conn_dwh.commit()

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