import datetime as dt

from airflow import DAG

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator


dag = DAG(
    dag_id="my_first_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 8, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)

psql_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="read_postgres_data_to_bucket",
    postgres_conn_id="postgres_gcp",
    sql="select * from gdd.land_registry_price_paid_uk where transfer_date = '{{ ds }}'",
    bucket="europe-west1-training-airfl-d9a9700f-data",
    filename="data/{{ ds_nodash }}-psql-land-registry-data.json",
    dag=dag,
)
