import psycopg2
import pandas as pd
from psycopg2 import sql
from data_warehouse.database_connection.config import load_config, load_config_gcloud



def get_season(start_date):
    month = start_date.month
    if 3 <= month <= 5:
        return "Spring"
    elif 6 <= month <= 8:
        return "Summer"
    elif 9 <= month <= 11:
        return "Autumn"
    else:
        return "Winter"

def fill_table_dim_Date():

    try:
        print("Connection in progress...")
        configure = load_config_gcloud()
        print('Loading in progress...\n')
        with psycopg2.connect(**configure) as conn:
            with conn.cursor() as cur:
                # sql query to truncate table
                del_query = """
                TRUNCATE TABLE dim_date cascade;
                """
                cur.execute(del_query)

                # sql query to insert
                query = """
                INSERT INTO dim_Date (date, dayOfMonth, month, year, dayOfWeek, dayOfYear, weekday, monthName, season) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """

                start_date = pd.to_datetime("2024-01-01")
                end_date = pd.to_datetime("2024-12-31")

                while start_date <= end_date:
                    dayOfMonth = start_date.day
                    month = start_date.month
                    year = start_date.year
                    dayOfWeek = start_date.day_of_week
                    dayOfYear = start_date.day_of_year
                    weekday = start_date.strftime('%A')
                    monthName = start_date.strftime('%B')

                    season = get_season(start_date)

                    # Execute the INSERT query
                    cur.execute(query, (
                    start_date, dayOfMonth, month, year, dayOfWeek, dayOfYear, weekday, monthName, season))

                    # Commit the transaction
                    conn.commit()
                    start_date += pd.Timedelta(days=1)

    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error: {error}")

if __name__ == "__main__":
    fill_table_dim_Date()
    print()
    print('Loading in complete!\n')


