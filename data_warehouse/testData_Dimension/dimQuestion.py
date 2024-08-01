import psycopg2
import pandas as pd
from data_warehouse.database_connection.config import load_config_test, load_config2



try:
    print("Connection to ElectifyDB in progress...")
    con = load_config2()

    print('Loading ElectifyDB in progress...\n')
    with psycopg2.connect(**con) as conn:
        with conn.cursor() as cur:
            # sql query to retrieve data from question table in the operational database
            sql = """
                            SELECT question_type, question_content 
                            FROM question
                            WHERE question_type IS NOT NULL;  -- Exclude rows with null question_type
                           """
            questionInfo = pd.read_sql(sql, conn)

        print("Connection to test DWH in progress...")
        configure = load_config_test()
        print('Loading in progress...\n')
        with psycopg2.connect(**configure) as conn:
            with conn.cursor() as cur:
                # sql query to truncate table
                del_query = """
                        TRUNCATE TABLE dim_Question CASCADE;
                        """
                cur.execute(del_query)

                # Insert into dim_Question table
                for index, row in questionInfo.iterrows():
                    # Fetch question_type_id based on question_type
                    cur.execute("SELECT question_type_id FROM dim_Question_Type WHERE question_type_name = %s",
                                (row['question_type'],))
                    question_type_id = cur.fetchone()
                    if question_type_id:
                        question_type_id = question_type_id[0]
                    else:
                        # Handle the case where no matching question_type_id is found
                        print(f"No matching question_type_id found for question_type: {row['question_type']}")
                        continue  # Skip this row and move to the next one

                    # Insert into dim_Question table with question_type_id
                    insert_query = """
                            INSERT INTO dim_Question (question_type_id, question_text) 
                            VALUES (%s, %s);
                            """
                    cur.execute(insert_query, (question_type_id, row['question_content']))

                # Commit the transaction
                conn.commit()

except (psycopg2.DatabaseError, Exception) as error:
    print(f"Error: {error}")

