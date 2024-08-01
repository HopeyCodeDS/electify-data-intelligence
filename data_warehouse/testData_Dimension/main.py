import subprocess
import psycopg2

# Run create_table.py
subprocess.run(["python", "create_test_tables.py"])

# Run dim_Date.py
subprocess.run(["python", "dimDate.py"])

# Run dim_Country.py
subprocess.run(["python", "dimCountry.py"])

# Run dim_Organization.py
subprocess.run(["python", "dimOrganization.py"])

# Run dim_Theme.py
subprocess.run(["python", "dimTheme.py"])

# Run dim_Question_type.py
subprocess.run(["python", "dimQuestion_type.py"])

# Run dim_Question.py
subprocess.run(["python", "dimQuestion.py"])

# Run Fact_survey.py
subprocess.run(["python", "factSurvey.py"])

# Run truncate_test_tables.py
subprocess.run(["python", "truncate_test_tables.py"])