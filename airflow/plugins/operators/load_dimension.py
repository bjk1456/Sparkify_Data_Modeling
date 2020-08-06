from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 load_dimension_table = "",
                 truncate_table = "",
                 truncate_table_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.load_dimension_table = load_dimension_table
        self.truncate_table = truncate_table
        self.truncate_table_query = truncate_table_query

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        if(self.truncate_table):
             cursor.execute(self.truncate_table_query)
             self.log.info(f"Inside self.truncate_table. The query is {self.truncate_table_query}")
        self.log.info("Connected to redshift ... load_dimension table")
        cursor.execute(self.load_dimension_table)
        cursor.close()
        conn.commit()
        
        return True