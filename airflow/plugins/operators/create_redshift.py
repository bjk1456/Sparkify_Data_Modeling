from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from ../helpers import sql_queries
from helpers import SqlQueries
from helpers import CreateTableQueries

class CreateRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 load_stagging_table = "",
                 delete_tables = False,
                 *args, **kwargs):

        super(CreateRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.delete_tables = delete_tables
        
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Connected to redshift ... CreateRedshiftOperator")
        
        if(self.delete_tables):
            for query in CreateTableQueries.drop_table_queries:
                cursor.execute(query)
                conn.commit()
        
        for query in CreateTableQueries.create_table_queries:
            cursor.execute(query)
            conn.commit()
            
        cursor.close()
        conn.commit()
        self.log.info("Create command completed ... CreateRedshiftOperator")

        return True
