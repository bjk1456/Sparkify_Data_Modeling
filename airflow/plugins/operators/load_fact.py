from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 load_fact_table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.load_fact_table = load_fact_table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        self.log.info("Connected to redshift ... LoadFactOperator")
        self.log.info(f'self.load_fact_table equals {self.load_fact_table}')
        cursor.execute(self.load_fact_table)
        cursor.close()
        conn.commit()
        self.log.info("Load command completed ... LoadFactOperator")
