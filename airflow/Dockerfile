FROM apache/airflow:2.9.3-python3.12

# Postgres provider gives us PostgresHook; pin to a 2.9-compatible release.
RUN pip install --no-cache-dir "apache-airflow-providers-postgres>=5.10,<6"
