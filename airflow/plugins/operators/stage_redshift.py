from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from ../helpers import sql_queries
from helpers import SqlQueries
from helpers import CreateTableQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 destination_table = "",
                 create_table_query = "",
                 load_stagging_table = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.destination_table = destination_table
        self.create_table_query = create_table_query
        self.load_stagging_table = load_stagging_table

        
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Connected to redshift ... StageToRedshiftOperator")
        cursor.execute(self.load_stagging_table)
        cursor.close()
        conn.commit()
        self.log.info("Load command completed ... StageToRedshiftOperator")

        return True
