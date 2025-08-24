import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
import json

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "PROCESSED_YEAR", "PROCESSED_MONTH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def get_db_config():
    secret_name = "dev/postgres/nyc_uber"
    client = boto3.client(
        service_name="secretsmanager",
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Unable to retrive secret - {str(e)}")
        raise e

    return get_secret_value_response["SecretString"]


db_config = json.loads(get_db_config())

jdbc_url = (
    f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
)
connection_properties = {
    "user": db_config["username"],
    "password": db_config["password"],
    "driver": "org.postgresql.Driver",
}

glue_database = "db_nyc_uber"

# Create If Not exists - Dimenstion tables
dim_tables_list = [
    "dim_vendors",
    "dim_ratecode",
    "dim_store_and_fwd_flag",
    "dim_payment_type",
    "dim_trip_peak_band",
    "dim_date",
    "dim_time",
    "dim_taxi_zone_lookup",
]
for dim_table in dim_tables_list:
    print(dim_table)
    table_check = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT tablename FROM pg_catalog.pg_tables where schemaname = 'public' and tablename='{dim_table}')",
        properties=connection_properties,
    )
    if table_check.count() == 0:
        df = glueContext.create_data_frame.from_catalog(
            database=glue_database, table_name=dim_table
        )
        df.write.jdbc(url=jdbc_url, table=dim_table, properties=connection_properties)
        print(f"DIM Table - {dim_table} - is created!")
    else:
        print(f"DIM Table - {dim_table} - already exists!")

# Loading Fact Table
fact_table_name = "fact_uber_trips"
df = glueContext.create_data_frame_from_catalog(
    database=glue_database, table_name=fact_table_name
)
PROCESSED_YEAR = args["PROCESSED_YEAR"]
PROCESSED_MONTH = args["PROCESSED_MONTH"]
df = df.filter(
    f" processed_month = '{PROCESSED_MONTH}' and processed_year = '{PROCESSED_YEAR}' "
)
df.write.mode("append").jdbc(
    url=jdbc_url, table=fact_table_name, properties=connection_properties
)
print(f"Fact Table - {fact_table_name} - is loaded!")


job.commit()
