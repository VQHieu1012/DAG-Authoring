FROM quay.io/astronomer/astro-runtime:12.2.0

ENV AIRFLOW_VAR_MY_DAG_JSON='{"name":"partner_a","api_secret":"api_secret"}'