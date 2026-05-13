FROM apache/airflow:2.9.3

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    requests \
    pandas \
    pyarrow \
    psycopg2-binary \
    sqlalchemy \
    dbt-core==1.8.8 \
    dbt-postgres==1.8.2 \
    great_expectations==0.18.21
