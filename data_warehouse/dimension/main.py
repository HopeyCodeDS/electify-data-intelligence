import subprocess
import psycopg2

# Run create_table.py
subprocess.run(["python", "create_tables.py"])

# Run dim_Date.py
subprocess.run(["python", "dim_Date.py"])

# Run dim_Country.py
subprocess.run(["python", "dim_Country.py"])

# Run dim_Organization.py
subprocess.run(["python", "dim_Organization.py"])

# Run dim_Theme.py
subprocess.run(["python", "dim_Theme.py"])

# Run dim_SubTheme.py
subprocess.run(["python", "dim_SubTheme.py"])

# Run dim_Question_type.py
subprocess.run(["python", "dim_Question_type.py"])

# Run dim_Question.py
subprocess.run(["python", "dim_Question.py"])

# Run fact_survey.py
subprocess.run(["python", "fact_survey.py"])

# Run survey_country_bridge.py
subprocess.run(["python", "survey_country_bridge.py"])

# Run survey_question_bridge.py
subprocess.run(["python", "survey_question_bridge.py"])