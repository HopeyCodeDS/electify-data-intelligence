import psycopg2
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config_localDWH, load_config_localDB, load_config_prodDWH, load_config_prodDB


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


def create_table_dim_subtheme(cursor):
    """Create the dim_SubTheme table"""
    command = """
        CREATE TABLE dim_SubTheme (
            subtheme_id SERIAL PRIMARY KEY,
            subtheme_name VARCHAR(255) NOT NULL
        )
    """
    table_name = "dim_SubTheme"
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



def create_table_fact_survey(cursor):
    """Create the Fact_Survey table"""
    command = """
        CREATE TABLE Fact_Survey (
            survey_id SERIAL PRIMARY KEY , -- Surrogate key
            date_id INT,
            theme_id INT,
            subtheme_id INT,
            organization_id INT,
            number_of_questions INT,
            FOREIGN KEY (date_id) REFERENCES dim_Date(date_id),
            FOREIGN KEY (theme_id) REFERENCES dim_Theme(theme_id),
            FOREIGN KEY (subtheme_id) REFERENCES dim_SubTheme(subtheme_id),
            FOREIGN KEY (organization_id) REFERENCES dim_Organization(organization_id)
        )
    """
    table_name = "Fact_Survey"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_survey_Question_bridge (cursor):
    """Create the Survey_Question_Bridge table"""
    command = """
        CREATE TABLE Survey_Question_Bridge (
            survey_id int,
            question_id int,
            PRIMARY KEY (survey_id, question_id),
            FOREIGN KEY (survey_id) REFERENCES Fact_Survey(survey_id) ON DELETE CASCADE,
            FOREIGN KEY (question_id) REFERENCES dim_Question(question_id) ON DELETE CASCADE
        )
    """
    table_name = "Survey_Question_Bridge"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_table_survey_country_bridge (cursor):
    """Create the Survey_Country_Bridge table"""
    command = """
        CREATE TABLE Survey_Country_Bridge (
            survey_id int,
            country_id int,
            PRIMARY KEY (survey_id, country_id),
            FOREIGN KEY (survey_id) REFERENCES Fact_Survey(survey_id) ON DELETE CASCADE,
            FOREIGN KEY (country_id) REFERENCES dim_Country(country_id) ON DELETE CASCADE
        )
    """
    table_name = "Survey_Country_Bridge"
    if not table_exists(cursor, table_name):
        cursor.execute(command)
        print(f"Table '{table_name}' created successfully.")
    else:
        print(f"Table '{table_name}' already exists, skipping creation.")


def create_tables():
    """Create all tables in the PostgreSQL database"""
    try:
        config = load_config_prodDWH()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                create_table_dim_date(cur)
                create_table_dim_theme(cur)
                create_table_dim_subtheme(cur)
                create_table_dim_country(cur)
                create_table_dim_organization(cur)
                create_table_dim_question_type(cur)
                create_table_dim_question(cur)
                create_table_fact_survey(cur)
                create_table_survey_Question_bridge(cur)
                create_table_survey_country_bridge(cur)
        print("All tables created successfully.")
    except psycopg2.DatabaseError as db_error:
        print(f"Database error: {db_error}")
    except Exception as error:
        print(f"An error occurred: {error}")


if __name__ == "__main__":
    create_tables()
