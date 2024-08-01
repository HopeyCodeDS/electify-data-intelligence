import psycopg2
from data_warehouse.database_connection.config import load_config2

def add_updated_at_column(table_name):
    commands = [
        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
        """
        CREATE OR REPLACE FUNCTION update_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        f"""
        CREATE TRIGGER set_timestamp_{table_name}
        BEFORE UPDATE ON {table_name}
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
        """
    ]

    try:
        config = load_config2()
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                for command in commands:
                    cursor.execute(command)
            conn.commit()
            print(f"Updated {table_name} table successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error updating {table_name} table: {error}")

if __name__ == "__main__":
    tables = ['questionnaire', 'question', 'organization', 'answer']  # Add other tables as needed
    for table in tables:
        add_updated_at_column(table)
