import psycopg2
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config, load_config2, load_config_gcloud


def table_exists(cursor, table_name):
    """Check if a table exists in the PostgreSQL database"""
    exists_query = sql.SQL(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)")
    cursor.execute(exists_query, [table_name])
    return cursor.fetchone()[0]


def create_table_dim_date(cursor):
    """Create the dim_Date table"""
    command = """
        CREATE TABLE dim_Date (
            date_id SERIAL PRIMARY KEY,
            date date,
            dayOfMonth int,
            month int,
            year int,
            dayOfWeek int,
            dayOfYear int,
            weekday VARCHAR(255) NOT NULL,
            monthName VARCHAR(255) NOT NULL,
            season VARCHAR(255) NOT NULL
        )
    """
    table_name = "dim_Date"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_dim_theme(cursor):
    """Create the dim_Theme table"""
    command = """
        CREATE TABLE dim_Theme (
            theme_id SERIAL PRIMARY KEY,
            theme_name VARCHAR(255) NOT NULL,
            date_created date
        )
    """
    table_name = "dim_Theme"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_dim_country(cursor):
    """Create the dim_Country table"""
    command = """
        CREATE TABLE dim_Country (
            country_id SERIAL PRIMARY KEY,
            country_name VARCHAR(255) NOT NULL
        )
    """
    table_name = "dim_Country"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_dim_organization(cursor):
    """Create the dim_Organization table"""
    command = """
        CREATE TABLE dim_Organization (
            organization_id SERIAL PRIMARY KEY,
            organization_name VARCHAR(255) NOT NULL
        )
    """
    table_name = "dim_Organization"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_dim_question_type(cursor):
    """Create the dim_Question_Type table"""
    command = """
        CREATE TABLE dim_Question_Type (
            question_type_id SERIAL PRIMARY KEY,
            question_type_name VARCHAR(255) NOT NULL
        )
    """
    table_name = "dim_Question_Type"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_dim_question(cursor):
    """Create the dim_Question table"""
    command = """
        CREATE TABLE dim_Question (
            question_id SERIAL PRIMARY KEY,
            question_type_id int NOT NULL,
            question_text VARCHAR(255),
            FOREIGN KEY (question_type_id) REFERENCES dim_Question_Type(question_type_id)
        )
    """
    table_name = "dim_Question"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_dim_answer(cursor):
    """Create the dim_Answer table"""
    command = """
        CREATE TABLE dim_Answer (
            answer_id SERIAL PRIMARY KEY,
            answer_type_name VARCHAR(255) NOT NULL
        )
    """
    table_name = "dim_Answer"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_fact_survey(cursor):
    """Create the Fact_Survey table"""
    command = """
        CREATE TABLE Fact_Survey (
            date_id int,
            theme_id int,
            organization_id int,
            question_id int,
            answer_id int,
            country_id int,
            number_of_questions_answered int,
            FOREIGN KEY (date_id) REFERENCES dim_Date(date_id),
            FOREIGN KEY (theme_id) REFERENCES dim_Theme(theme_id),
            FOREIGN KEY (organization_id) REFERENCES dim_Organization(organization_id),
            FOREIGN KEY (question_id) REFERENCES dim_Question(question_id),
            FOREIGN KEY (country_id) REFERENCES dim_Country(country_id)
        )
    """
    table_name = "Fact_Survey"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_etl_tracking(cursor):
    """Create the etl_tracking table"""
    command = """
        CREATE TABLE etl_tracking (
            table_name VARCHAR(255) PRIMARY KEY,
            last_load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    table_name = "etl_tracking"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_tables():
    """Create all tables in the PostgreSQL database"""
    try:
        config = load_config_gcloud()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                create_table_dim_date(cur)
                create_table_dim_theme(cur)
                create_table_dim_country(cur)
                create_table_dim_organization(cur)
                create_table_dim_question_type(cur)
                create_table_dim_question(cur)
                create_table_dim_answer(cur)
                create_table_fact_survey(cur)
                create_table_etl_tracking(cur)
        print("All tables created successfully.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    except Exception as error:
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    create_tables()
