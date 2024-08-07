import psycopg2
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config, load_config2

schema = "public"


def table_exists(cursor, table_name):
    """Check if a table exists in the PostgreSQL database"""
    cursor.execute(sql.SQL(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)"),
        [schema, table_name]
    )
    return cursor.fetchone()[0]


def dropTable(cursor, connection, table_name):
    """Drop a table if it exists"""
    if table_exists(cursor, table_name):
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        ))
        connection.commit()
        print(f"Table '{table_name}' dropped successfully.")
    else:
        print(f"Table '{table_name}' does not exist, skipping drop.")


def drop_all_tables(cursor, connection):
    """Drop all tables in the correct order"""
    tables = ["fact_survey", "dim_Date", "dim_Theme", "dim_Country", "dim_Organization",
              "dim_Question_Type", "survey_question_bridge", "survey_country_bridge"]
    # tables = ["fact_survey"]

    for table in tables:
        dropTable(cursor, connection, table)


if __name__ == "__main__":
    try:
        config = load_config()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # Drop all tables
                drop_all_tables(cur, conn)
                print("All tables processed successfully.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    except Exception as error:
        print(f"An error occurred: {error}")
