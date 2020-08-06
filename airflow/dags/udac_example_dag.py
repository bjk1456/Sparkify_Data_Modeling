from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateRedshiftOperator)
from airflow.models import Variable
from helpers import SqlQueries
from helpers import CreateTableQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.now(),
          schedule_interval="@hourly"
        )
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stop_operator = DummyOperator(task_id='End_execution',  dag=dag)

create_redshift_tables = CreateRedshiftOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="staging_events",
    json_format = "s3://udacity-dend/log_json_path.json",
    load_stagging_table = SqlQueries.COPY_EVENTS_SQL.format()
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    destination_table="staging_songs",
    load_stagging_table = SqlQueries.COPY_SONGS_SQL.format()
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    load_fact_table = SqlQueries.songplay_table_insert.format()
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    truncate_table=True,
    truncate_table_query=SqlQueries.TRUNCATE_TABLE.format("users"),
    load_dimension_table=SqlQueries.user_table_insert.format()
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    truncate_table=True,
    truncate_table_query=SqlQueries.TRUNCATE_TABLE.format("songs"),
    load_dimension_table = SqlQueries.song_table_insert.format()
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    truncate_table=True,
    truncate_table_query=SqlQueries.TRUNCATE_TABLE.format("artists"),
    load_dimension_table = SqlQueries.artist_table_insert.format()
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    truncate_table=True,
    truncate_table_query=SqlQueries.TRUNCATE_TABLE.format("time"),
    load_dimension_table = SqlQueries.time_table_insert.format()
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tests = SqlQueries.tests
)

start_operator >> create_redshift_tables

create_redshift_tables >> stage_events_to_redshift 

create_redshift_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table

stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table

load_songplays_table >> load_song_dimension_table

load_songplays_table >> load_artist_dimension_table

load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks

load_song_dimension_table >> run_quality_checks

load_artist_dimension_table >> run_quality_checks

load_time_dimension_table >> run_quality_checks

run_quality_checks >> stop_operator
