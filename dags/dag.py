import datetime as dt

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

from http_to_gcs_operator import HttpToGcsOperator

my_second_dag = DAG(
    dag_id="my_second_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "atigelaar@bol.com",
    },
)

with my_second_dag as dag:
    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id="read_postgres_data_to_bucket",
        postgres_conn_id="postgres_gcp",
        sql="select * from land_registry_price_paid_uk where transfer_date = '{{ ds }}'",
        bucket="europe-west1-training-airfl-d9a9700f-data",
        filename="data/{{ ds }}-psql-land-registry-data.json")

    cluster_name = "my-dataproc-cluster-{{ ds }}"
    project_id = "airflowbolcom-656e0a307aa4039f"
    zone = "europe-west4-a"

    dataproc_create_cluster = DataprocClusterCreateOperator(
        task_id="dataproc_cluster_spinup",
        cluster_name=cluster_name,
        project_id=project_id,
        num_workers=2,
        zone=zone)

    compute_aggregates = DataProcPySparkOperator(
        task_id="dataproc_compute_aggs",
        main="gs://europe-west1-training-airfl-d9a9700f-bucket/other/build_statistics_simple.py",
        cluster_name=cluster_name,
        arguments=["{{ ds }}"])

    dataproc_delete_cluster = DataprocClusterDeleteOperator(
        task_id="dataproc_cluster_spindown",
        cluster_name=cluster_name,
        project_id=project_id,
        trigger_rule=TriggerRule.ALL_DONE)

    table = "airflowtrain_a.{{ ds_nodash }}_landregistry"
    gcs_to_bq_task = GoogleCloudStorageToBigQueryOperator(
        task_id="gcs_to_bq_import",
        bucket="europe-west1-training-airfl-d9a9700f-data",
        source_objects=["average_prices/transfer_date={{ ds }}/*.parquet"],
        destination_project_dataset_table=table,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE")

    #
    # transform_currency_task = PythonOperator(
    #     task_id='test', python_callable=transform_currency,
    #     op_kwargs={'base_dir': base_dir}, dag=dag)

    psql_to_gcs >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
    gcs_to_bq_task.set_upstream(compute_aggregates)

    for currency in [ "EUR", "USD" ]:
        currency_transform_task = HttpToGcsOperator(
            task_id="get_currency_" + currency,
            http_conn_id="http_default",
            gcs_conn_id="gcs_default",
            endpoint="https://europe-west1-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=EUR",
            gcs_path="gs://europe-west1-training-airfl-d9a9700f-data/currencies/{{ ds }}-currency-" + currency + ".json"
        )
        compute_aggregates >> currency_transform_task
