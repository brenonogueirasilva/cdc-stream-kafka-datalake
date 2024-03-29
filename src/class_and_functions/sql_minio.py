import duckdb
import logging
from pandas import DataFrame

class SQLMinio:
    """
    A class for interacting with Minio data using SQL queries.
    Attributes:
        host (str): The hostname of the Minio server.
        port (int): The port number of the Minio server.
        access_key (str): The access key for authentication.
        secret_key (str): The secret key for authentication.
        cursor (duckdb.Cursor): DuckDB cursor for executing SQL queries.
    """
    def __init__(self, host: str, port: int, access_key: str, secret_key: str):
            self.host = host 
            self.porta = port 
            self.access_key = access_key
            self.secret_key = secret_key
            self.cursor = self.cursor_iniciation() 

    def cursor_iniciation(self) -> duckdb.Cursor:
        """
        Initializes a DuckDB cursor for executing SQL queries.
        Returns:
            duckdb.Cursor: The DuckDB cursor instance.
        """            
        cursor = duckdb.connect()
        cursor.execute("INSTALL httpfs")
        cursor.execute("LOAD httpfs")
        cursor.execute("SET s3_region='us-east-1'")
        cursor.execute("SET s3_url_style='path'")
        cursor.execute("SET s3_use_ssl= false")
        cursor.execute(f"SET s3_endpoint='{self.host}:{self.porta}'")
        cursor.execute(f"SET s3_access_key_id='{self.access_key}'")
        cursor.execute(f"SET s3_secret_access_key='{self.secret_key}'")
        return cursor
    
    def sql_query_to_dataframe(self, sql_query: str) -> DataFrame:
        """
        Executes an SQL query and returns the result as a Pandas DataFrame.
        Args:
            sql_query (str): The SQL query to execute.
        Returns:
            DataFrame: The result of the SQL query as a DataFrame.
        """
        try:
            logging.info('Query Executed with Sucess')
            return self.cursor.sql(sql_query).df()
        except Exception as e:
            logging.error('Error executing query:', str(e))

    def executing_query(self, sql_query: str):
        """
        Executes an SQL query without returning a result.
        Args:
            sql_query (str): The SQL query to execute.
        """
        try:
            self.cursor.sql(sql_query)
            logging.info('Query Executed with Sucess')
        except Exception as e:
            logging.error('Error executing query:', str(e))

    def generate_sql_select_minio_lake(self, bucket: str, destination_folder: str, filter_data_partition: str = None) -> str:
        """
        Generates an SQL query to select data from a Minio lake.
        Args:
            bucket (str): The name of the Minio bucket.
            destination_folder (str): The destination folder in the Minio bucket.
            filter_data_partition (str, optional): The data partition to filter on.
        Returns:
            str: The generated SQL query.
        """
        if filter_data_partition == None:
            sql_lake = f'''
                SELECT * 
                FROM read_parquet('s3://{bucket}/{destination_folder}/*/*.parquet')
            '''
        else:
            sql_lake = f'''
                SELECT * 
                FROM read_parquet('s3://{bucket}/{destination_folder}/*/*.parquet')
                WHERE data_partition = '{filter_data_partition}'
            '''
        return sql_lake 

    def generate_sql_insert_minio_lake(self, sql_query: str, bucket: str, pasta_destino: str) -> str:
        """
        Generates an SQL query to insert data into a Minio lake.
        Args:
            sql_query (str): The SQL query to retrieve data to insert.
            bucket (str): The name of the Minio bucket.
            pasta_destino (str): The destination folder in the Minio bucket.
        Returns:
            str: The generated SQL query.
        """
        sql_insert = f'''
            COPY (
                {sql_query}
            ) to 's3://{bucket}/{pasta_destino}/' (FORMAT PARQUET, OVERWRITE_OR_IGNORE, PARTITION_BY (data_partition) )
            '''
        return sql_insert 

    def generate_sql_union(self, sql1: str, sql2: str) -> str:
        """
        Generates an SQL query to perform a union of two SQL queries.
        Args:
            sql1 (str): The first SQL query.
            sql2 (str): The second SQL query.
        Returns:
            str: The generated SQL query.
        """
        sql_uniao = f'''
            {sql1} 
            UNION
            {sql2}
        '''
        return sql_uniao

       