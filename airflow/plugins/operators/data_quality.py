from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 tests="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tests = tests

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info('DataQualityOperator about to execute')

        for row in self.tests:
            records = redshift_hook.get_records(row['test'])
            self.log.info(f"records[0][0] == {records[0][0]}")
            if(int(records[0][0]) != int(row['expected_result'])):
                raise ValueError(f"Data quality check failed. {row['test']} should be {row['expected_result']} but is {records[0][0]}")
                logging.info(f"Data quality check failed. {row['test']} should be {row['expected_result']} but is {records[0][0]}")
        self.log.info('DataQualityOperator has executed!')