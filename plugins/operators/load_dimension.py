from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql
        self.append_data = append_data

    def execute(self, context):
        self.log.info('Loading data to table {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            formatted_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.sql
            )
            redshift.run(formatted_sql)
        else:
            formatted_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.sql
            )
            
            self.log.info("Clearing data from dimension table in Redshift")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
            self.log.info("Loading data into dimension table in Redshift")
            redshift.run(formatted_sql)
        
