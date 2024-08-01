import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config, load_config2

try:
    print("Connection to ElectifyDB in progress...")
    con = load_config2()

    print('Loading ElectifyDB in progress...\n')
    with psycopg2.connect(**con) as conn:
        with conn.cursor() as cur:
            # sql query to retrieve data from question table in the operational database
            sql = """
                            SELECT answer_type 
                            FROM answer
                            JOIN question ON answer.question_id = question.id
                            WHERE question_type IS NOT NULL;  -- Exclude rows with null question_type
                           """
            answerInfo = pd.read_sql(sql, conn)
            print(answerInfo)

        print("Connection to DWH in progress...")
        configure = load_config()
        print('Loading in progress...\n')
        with psycopg2.connect(**configure) as conn:
            with conn.cursor() as cur:
                # sql query to truncate table
                del_query = """
                    TRUNCATE TABLE dim_Answer cascade;
                    """
                cur.execute(del_query)

                # sql query to insert
                insert_query = """
                    INSERT INTO dim_Answer (answer_type_name)
                    VALUES (%s);
                    """

                # Execute the INSERT query
                for index, row in answerInfo.iterrows():
                    cur.execute(insert_query, (row['answer_type'],))

                # Commit the transaction
                conn.commit()

except (psycopg2.DatabaseError, Exception) as error:
    print(f"Error: {error}")

