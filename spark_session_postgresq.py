# Import your modules
import findspark
import boto3
import sys
from pyspark.sql import SparkSession
import psycopg2
import os
from pyspark.sql.functions import count,col,asc,desc
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType
findspark.init()

# Create a spark session

# Set the path to the PostgreSQL JDBC driver JAR file
spark_jars_path = r"C:\User\spark\spark-3.4.0-bin-hadoop3\jars\postgresql-42.6.0.jar"

# Configure Spark to use the PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("Create PostgreSQL Database") \
    .config("spark.driver.extraClassPath", spark_jars_path) \
    .getOrCreate()

print('session created')

# Configure AWS credentials the hard way

aws_access_key_id = "your_access_key"
aws_secret_access_key = "your_secret_key"

# Configure your AWS credentials the with os getenv

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

if not aws_access_key_id or not aws_secret_access_key:
    raise ValueError("AWS credentials are not set as environment variables.")

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Replace 'your_bucket_name', 'path/to/your/file.csv', and 'your_file.csv' with your actual S3 bucket name, file path, and file name.

bucket_name = 'your_bucket_name'
file_path = 'path/to/your/file.csv'
file_name = 'your_file.csv'

# Create an S3 client
s3_client = session.client('s3')

# Read the file from S3 and store it as a DataFrame using pandas
csv_object = s3_client.get_object(Bucket=bucket_name, Key=file_path)
df = pd.read_csv(csv_object['Body'])

# Now, 'df' contains the data from the CSV file as a pandas DataFrame
print(df.head())

# Or You can read a csv file from any folder just the test the process

df = spark.read.csv('data_full.csv',encoding='utf-8',sep=',', header=True, inferSchema=True)

# You can also define a parquet file from a s3 bucket

s3_parquet_path = "s3://your-bucket-name/path/to/data_full.parquet"

# Then read the Parquet file as a DataFrame

df = spark.read.parquet(s3_parquet_path)

# Show the DataFrame

df.show()
 
# You can check you dataframe schema as well
# Assuming you have a DataFrame named 'df'

df.printSchema()

# set postgres credencials hard way

postgres_host = "your host"
postgres_port = 'any suitable port'
postgres_database = "yourdatabase"
postgres_user = "youruser"
postgres_password = "yourpassword"
new_postgres_database='postgres'
new_schema = "schema_v1"
new_table = "tabela_estudos_v1"

# Create a connection to the default database

default_conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    dbname="postgres",
    user=postgres_user,
    password=postgres_password
)
default_conn.autocommit = True

# Connect to the newly created database

conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    dbname=new_postgres_database,
    user=postgres_user,
    password=postgres_password
)
conn.autocommit = True

# Create a cursor to execute SQL statements

cursor = conn.cursor()

# Mapping between Spark data types and PostgreSQL data types

type_mapping = {
    StringType(): "TEXT",
    IntegerType(): "INTEGER",
    FloatType(): "FLOAT",
    DoubleType(): "DOUBLE PRECISION",
    BooleanType(): "BOOLEAN",
    DateType(): "DATE",
    TimestampType(): "TIMESTAMP"
}

# Use the DataFrame schema to create the table in the database

table_columns = ", ".join([f"{field.name} {type_mapping.get(field.dataType, 'TEXT')}" for field in df.schema.fields])
create_table_query = f"CREATE TABLE IF NOT EXISTS schema_v1.{new_table} ({table_columns})"
cursor.execute(create_table_query)

# Insert the data from the DataFrame into the table

df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/postgres") \
    .option("dbtable", f"schema_v1.{new_table}") \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .mode("append") \
    .save()

# Close the cursor and connection

cursor.close()
conn.close()

# Stop the SparkSession

spark.stop()conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    dbname=new_postgres_database,
    user=postgres_user,
    password=postgres_password
)
conn.autocommit = True
