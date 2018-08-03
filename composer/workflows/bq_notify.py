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
bq_dataset_name = 'chicago_historical_congestion_data'
bq_temp_composer_dataset = bq_dataset_name + '.table_exported_by_composer'
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
    # [END composer_notify_failure]

    # [START composer_bash_bq]
    # Create BigQuery output dataset.
   # make_bq_dataset = bash_operator.BashOperator(
    #    task_id='make_bq_dataset',
        # Executing 'bq' command requires Google Cloud SDK which comes
        # preinstalled in Cloud Composer.
    #    bash_command='bq ls {} || bq mk {}'.format(
    #        bq_dataset_name, bq_dataset_name))
    # [END composer_bash_bq]

    # [START composer_bigquery]
    # Query recent StackOverflow questions.
    bq_recent_questions_query = bigquery_operator.BigQueryOperator(
        task_id='bq_recent_questions_query',
        bql="""
          SELECT CURRENT_SPEED,REGION_ID FROM `camel-154800.chicago_historical_congestion_data.anthill_view` LIMIT 1000  
        """.format(max_date=max_query_date, min_date=min_query_date),
        use_legacy_sql=False,
        destination_dataset_table=bq_temp_composer_dataset)
    # [END composer_bigquery]

    # Export query result to Cloud Storage.
    export_questions_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_recent_questions_to_gcs',
        source_project_dataset_table=bq_temp_composer_dataset,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV')

    # Perform most popular question query.
#    bq_most_popular_query = bigquery_operator.BigQueryOperator(
 #       task_id='bq_most_popular_question_query',
 #       bql="""
  #      SELECT title, view_count
 #       FROM `{table}`
  #      ORDER BY view_count DESC
  #      LIMIT 1
  #      """.format(table=bq_recent_questions_table_id),
  #      use_legacy_sql=False,
  #      destination_dataset_table=bq_most_popular_table_id)

    # Read most popular question from BigQuery to XCom output.
    # XCom is the best way to communicate between operators, but can only
    # transfer small amounts of data. For passing large amounts of data, store
    # the data in Cloud Storage and pass the path to the data if necessary.
    # https://airflow.apache.org/concepts.html#xcoms
   # bq_read_most_popular = bigquery_get_data.BigQueryGetDataOperator(
   #     task_id='bq_read_most_popular',
   #     dataset_id=bq_dataset_name,
   #     table_id=BQ_MOST_POPULAR_TABLE_NAME)

    # [START composer_email]
    # Send email confirmation
    #email_summary = email_operator.EmailOperator(
     #   task_id='email_summary',
     #   to=models.Variable.get('email'),
     #   subject='Sample BigQuery notify data ready',
     #   html_content="""
     #   Analyzed Stack Overflow posts data from  12AM to 
     #   12AM. The most popular question was someone with
     #   views. Top 100 questions asked are now available at:.
     #   """.format()
    # [END composer_email]

    # Delete BigQuery dataset
    # Delete the bq table
   # delete_bq_dataset = bash_operator.BashOperator(
    #    task_id='delete_bq_dataset',
    #    bash_command='bq rm -r -f %s' % bq_dataset_name,
    #    trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Define DAG dependencies.
    #(
        #make_bq_dataset
    bq_recent_questions_query >> export_questions_to_gcs #>> email_summary
     #   >> delete_bq_dataset
    #)
    #(
    #    bq_recent_questions_query
    #    >> bq_most_popular_query
    #    >> bq_read_most_popular
    #    >> delete_bq_dataset
    #)
    #export_questions_to_gcs >> email_summary
    #bq_read_most_popular >> email_summary
