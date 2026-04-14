FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y git

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-slack \
    apache-airflow-providers-amazon

RUN pip install --no-cache-dir \
    pandas \
    boto3 \
    snowflake-connector-python

RUN pip install --no-cache-dir \
    dbt-snowflake --no-deps

RUN pip install --no-cache-dir \
    dbt-core \
    dbt-adapters
