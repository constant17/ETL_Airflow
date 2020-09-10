from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS json '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3="",
                 s3_key="",
                 json_file="",
                 region = "",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3=s3
        self.s3_key=s3_key
        self.json_file=json_file
        self.region = region
        self.table=table

    def execute(self, context):
        self.log.info("Setting up Redshift connection...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Getting key..")
        key = self.s3_key.format(**context)
        self.log.info("Key Gotten!")
        self.log.info("Formatting key with s3..")
        s3 = "s3://{}/{}".format(self.s3, key)
        self.log.info("Formatting Done!")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_file
        )
        self.log.info("Running Query...")
        redshift.run(formatted_sql)
        self.log.info("Query Successfuly Executed")




