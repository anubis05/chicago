#DAGs are defined for Cloud COmposer AKA Apache Airflow. 
# Ideally we want a series of activities to happen when we fetch the data, analyze the data and then
# notify people about the data
#This was never fully functional


import datetime

# [START composer_notify_failure]
from airflow import models
# [END composer_notify_failure]
from airflow.contrib.operators import bigquery_get_data
# [START composer_bigquery]
from airflow.contrib.operators import bigquery_operator
# [END composer_bigquery]
from airflow.contrib.operators import bigquery_to_gcs
# [START composer_bash_bq]
from airflow.operators import bash_operator
# [END composer_bash_bq]
# [START composer_email]
from airflow.operators import email_operator
# [END composer_email]
from airflow.utils import trigger_rule


#bq_dataset_name = 'airflow_bq_notify_dataset_{{ ds_nodash }}'
bq_dataset_name = 'composer'
bq_temp_composer_dataset = bq_dataset_name + '.table_exported_by_composer'
bq_composer_final_output = bq_dataset_name + '.table_final_output'
output_file = 'gs://{gcs_bucket}/exported_from_bq.csv'.format(
    gcs_bucket=models.Variable.get('gcs_bucket'))


# Data from the month of January 2018
# You may change the query dates to get data from a different time range. You
# may also dynamically pick a date range based on DAG schedule date. Airflow
# macros can be useful for this. For example, {{ macros.ds_add(ds, -7) }}
# corresponds to a date one week (7 days) before the DAG was run.
# https://airflow.apache.org/code.html?highlight=execution_date#airflow.macros.ds_add

max_query_date = '2018-02-01'
min_query_date = '2018-01-01'

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# [START composer_notify_failure]
default_dag_args = {
    'start_date': yesterday,
    # Email whenever an Operator in the DAG fails.
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'composer_sample_bq_notify',
        schedule_interval=datetime.timedelta(weeks=4),
        default_args=default_dag_args) as dag: 

    # Query recent today's data
    bq_fetch_todays_data = bigquery_operator.BigQueryOperator(
        task_id='bq_fetch_todays_data',
        bql="""
          SELECT * FROM `camel-154800.chicago_historical_congestion_data.Aug2_Larger_dataset_view`  WHERE FULL_DATE=CURRENT_DATE("America/Chicago") ORDER BY HH_hour DESC  
        """.format(max_date=max_query_date, min_date=min_query_date),
        use_legacy_sql=False,
        destination_dataset_table=bq_temp_composer_dataset)
    # [END composer_bigquery]

    # [START composer_bigquery]
  

    # Export query result to Cloud Storage.
    export_output_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_output_to_gcs',
        source_project_dataset_table=bq_temp_composer_dataset,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV')



    # Delete BigQuery dataset
    # Delete the bq table

        #make_bq_dataset
    bq_fetch_todays_data >> export_output_to_gcs
