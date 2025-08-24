import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.utils import *
from awsglue.dynamicframe import DynamicFrame
import boto3 as b
from pyspark import SparkFiles
from botocore.exceptions import ClientError
from datetime import datetime

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "SOURCE_RAW_FILE_PATH", "PROCESSED_YEAR", "PROCESSED_MONTH"]
)


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

glue = b.client("glue")
database_name = "db_nyc_uber"
dim_output_path = "s3://processed-data-bucket-5f593a/nyc-yellow-uber-data/dim/"
fact_input_path = args["SOURCE_RAW_FILE_PATH"]
fact_ouput_path = "s3://processed-data-bucket-5f593a/nyc-yellow-uber-data/fact/"

print(fact_input_path)


def check_db_exists(database_name):
    try:
        response = glue.get_database(Name=database_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return False
        else:
            print("Boto3 Client Error - " + str(e))
            raise e
    except Exception as e:
        print("Error - " + str(e))
        raise e


def create_db_if_not_exists(database_name):
    if not check_db_exists(database_name):
        glue.create_database(DatabaseInput={"Name": database_name})
        print(f"Database {database_name} is created!")
    else:
        print(f"Database {database_name} already exists!")


def load_dataframe_to_s3(df, output_path, database_name, table_name):
    dyf = DynamicFrame.fromDF(df, glueContext)
    s3output = glueContext.getSink(
        path=output_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        compression="snappy",
        enableUpdateCatalog=True,
    )
    s3output.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table_name)
    s3output.setFormat("glueparquet")
    s3output.writeFrame(dyf)


def check_table_exists(database_name, table_name):
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            return False
        else:
            print("Boto3 Client Error - " + str(e))
            raise e
    except Exception as e:
        print("Error - " + str(e))
        raise e


def create_table_if_not_exists(
    data, ouput_path, database_name, table_name, is_dataset=True
):
    if not check_table_exists(database_name, table_name):
        if is_dataset:
            df = spark.createDataFrame(data)
        else:
            df = data
        load_dataframe_to_s3(df, ouput_path + table_name, database_name, table_name)
        print(f"Table {database_name+'.'+table_name} is created and loaded with data!")
    else:
        print(f"Table {database_name+'.'+table_name} already exists!")


# Creating Database `db_nyc_uber` if not exists in Glue Catalog
create_db_if_not_exists(database_name)


# Creating Dimension Tables if not exists on pre configured values from the Requirement's Data Contract
dim_vendors = [
    {
        "vendor_id": "1",
        "vendor_name": "Creative Mobile Technologies, LLC",
    },
    {
        "vendor_id": "2",
        "vendor_name": "Curb Mobility, LLC",
    },
    {
        "vendor_id": "6",
        "vendor_name": "Myle Technologies Inc",
    },
    {"vendor_id": "7", "vendor_name": "Helix"},
]
create_table_if_not_exists(dim_vendors, dim_output_path, database_name, "dim_vendors")

dim_ratecode = [
    {"rate_code_id": "1", "rate_code_description": "Standard rate"},
    {"rate_code_id": "2", "rate_code_description": "JFK"},
    {"rate_code_id": "3", "rate_code_description": "Newark"},
    {"rate_code_id": "4", "rate_code_description": "Nassau or Westchester"},
    {"rate_code_id": "5", "rate_code_description": "Negotiated fare"},
    {"rate_code_id": "6", "rate_code_description": "Group ride"},
    {"rate_code_id": "99", "rate_code_description": "Null/unknown"},
]
create_table_if_not_exists(dim_ratecode, dim_output_path, database_name, "dim_ratecode")

dim_store_and_fwd_flag = [
    {
        "store_and_fwd_flag_id": "1",
        "store_and_fwd_flag_description": "store and forward trip",
    },
    {
        "store_and_fwd_flag_id": "0",
        "store_and_fwd_flag_description": "not a store and forward trip",
    },
]
create_table_if_not_exists(
    dim_store_and_fwd_flag, dim_output_path, database_name, "dim_store_and_fwd_flag"
)

dim_payment_type = [
    {"payment_type_id": "0", "payment_type_description": "Flex Fare trip"},
    {"payment_type_id": "1", "payment_type_description": "Credit card"},
    {"payment_type_id": "2", "payment_type_description": "Cash"},
    {"payment_type_id": "3", "payment_type_description": "No charge"},
    {"payment_type_id": "4", "payment_type_description": "Dispute"},
    {"payment_type_id": "5", "payment_type_description": "Unknown"},
    {"payment_type_id": "6", "payment_type_description": "Voided trip"},
]
create_table_if_not_exists(
    dim_payment_type, dim_output_path, database_name, "dim_payment_type"
)

dim_date = spark.sql(
    """
SELECT explode(sequence(to_date('2000-01-01'), to_date('2030-01-01'))) as date
"""
)

dim_date_cols = {
    "date_id": date_format(dim_date.date, "yMMdd"),
    "date": date_format(dim_date.date, "d"),
    "month": date_format(dim_date.date, "M"),
    "year": date_format(dim_date.date, "y"),
    "day_short": date_format(dim_date.date, "E"),
    "day_long": date_format(dim_date.date, "EEEE"),
    "month_short": date_format(dim_date.date, "LLL"),
    "month_long": date_format(dim_date.date, "LLLL"),
    "is_weekend": when(
        (date_format(dim_date.date, "EEE").isin("Sat", "Sun")), "Yes"
    ).otherwise("No"),
}
dim_date = dim_date.withColumns(dim_date_cols)
create_table_if_not_exists(
    dim_date, dim_output_path, database_name, "dim_date", is_dataset=False
)

dim_trip_peak_band = [
    {
        "trip_peak_band_id": "101",
        "trip_peak_band_description": "Night Hour",
        "trip_peak_band_id": "102",
        "trip_peak_band_description": "Peak Hour",
        "trip_peak_band_id": "103",
        "trip_peak_band_description": "Off-Peak",
    }
]
create_table_if_not_exists(
    dim_trip_peak_band, dim_output_path, database_name, "dim_trip_peak_band"
)

dim_time = spark.sql(
    """
SELECT explode(sequence(to_timestamp('2000-01-01 00:00:00'), to_timestamp('2000-01-01 23:59:59'), interval 1 second)) as date
"""
)
dim_time.createOrReplaceTempView("dim_time")
dim_time = spark.sql(
    """
                    select date_format(date,'HHmmss') time_id,
                    date_format(date, 'H') hour,
                    date_format(date, 'm') minute,
                    date_format(date, 's') second
                    from dim_time 
                    """
)
create_table_if_not_exists(
    dim_time, dim_output_path, database_name, "dim_time", is_dataset=False
)

taxi_zone_lookup_url = "s3://raw-data-bucket-5f593a/lookups/taxi_zone_lookup.csv"
dim_taxi_zone_lookup = spark.read.csv(taxi_zone_lookup_url, header=True)
create_table_if_not_exists(
    dim_taxi_zone_lookup,
    dim_output_path,
    database_name,
    "dim_taxi_zone_lookup",
    is_dataset=False,
)

# ETL - S3 File Upload Event processed via Lambda

df = spark.read.parquet(fact_input_path)

rename_column_dict = {
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "drop_off_location_id",
}

df = df.withColumnsRenamed(rename_column_dict)

cast_column_dict = {
    "vendor_id": col("vendor_id").cast(IntegerType()),
    "tpep_pickup_datetime": date_format(
        col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"
    ),
    "tpep_dropoff_datetime": date_format(
        col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"
    ),
    "passenger_count": col("passenger_count").cast(IntegerType()),
    "trip_distance": col("trip_distance").cast(FloatType()),
    "rate_code_id": col("rate_code_id").cast(IntegerType()),
    "store_and_fwd_flag": when(
        col("store_and_fwd_flag").cast(StringType()) == "Y", 1
    ).otherwise(0),
    "pickup_location_id": col("pickup_location_id").cast(IntegerType()),
    "drop_off_location_id": col("drop_off_location_id").cast(IntegerType()),
    "payment_type": col("payment_type").cast(IntegerType()),
    "fare_amount": col("fare_amount").cast(FloatType()),
    "extra": col("extra").cast(FloatType()),
    "mta_tax": col("mta_tax").cast(FloatType()),
    "tip_amount": col("tip_amount").cast(FloatType()),
    "improvement_surcharge": col("improvement_surcharge").cast(FloatType()),
    "total_amount": col("total_amount").cast(DecimalType(10, 2)),
    "congestion_surcharge": col("congestion_surcharge").cast(FloatType()),
    "airport_fee": coalesce(col("airport_fee").cast(FloatType()), lit(0.0)),
}
df = df.withColumns(cast_column_dict)

df = df.drop_duplicates()

df = df.filter(df.passenger_count >= 1).filter(df.passenger_count <= 6)

df = df.filter(df.trip_distance >= 5.0).filter(df.trip_distance <= 500.0)

df = df.filter("fare_amount > 0 ")

df = df.withColumn("trip_id", expr("uuid()"))

date_cols = {
    "tpep_pickup_date_id": date_format("tpep_pickup_datetime", "yyyyMMdd"),
    "tpep_pickup_time_id": date_format("tpep_pickup_datetime", "HHmmss"),
    "tpep_dropoff_date_id": date_format("tpep_dropoff_datetime", "yyyyMMdd"),
    "tpep_dropoff_time_id": date_format("tpep_dropoff_datetime", "HHmmss"),
}

df = df.withColumns(date_cols)

df = df.withColumn(
    "trip_duration_minutes",
    floor(
        (
            unix_timestamp(df["tpep_dropoff_datetime"])
            - unix_timestamp(df["tpep_pickup_datetime"])
        )
        / 60
    ),
)

df = df.filter(df["trip_duration_minutes"] < 1440)

df = df.withColumn("Hour", date_format("tpep_pickup_datetime", "HH"))

df_trip_peak_band = (
    df.select(date_format("tpep_pickup_datetime", "HH").alias("Hour"))
    .distinct()
    .select(
        "Hour",
        when(col("Hour").between(0, 5) | col("Hour").between(20, 23), "101")
        .when(col("Hour").between(6, 9) | col("Hour").between(16, 19), "102")
        .otherwise("103")
        .alias("trip_peak_band_id"),
    )
)

df = df.join(df_trip_peak_band, df_trip_peak_band.Hour == df.Hour).select(
    df["*"], df_trip_peak_band.trip_peak_band_id
)

df = df.drop("tpep_pickup_datetime", "tpep_dropoff_datetime", "Hour")

PROCESSED_YEAR = args["PROCESSED_YEAR"]
PROCESSED_MONTH = args["PROCESSED_MONTH"]

df = df.withColumn("processed_year", lit(PROCESSED_YEAR))
df = df.withColumn("processed_month", lit(PROCESSED_MONTH))

df = df.select(
    "trip_id",
    "vendor_id",
    "passenger_count",
    "tpep_pickup_date_id",
    "tpep_pickup_time_id",
    "tpep_dropoff_date_id",
    "tpep_dropoff_time_id",
    "trip_duration_minutes",
    "trip_peak_band_id",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "drop_off_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
    "processed_year",
    "processed_month",
)

dyf = DynamicFrame.fromDF(df, glueContext)
s3output = glueContext.getSink(
    path=fact_ouput_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["processed_year", "processed_month"],
    compression="snappy",
    enableUpdateCatalog=True,
)
s3output.setCatalogInfo(
    catalogDatabase=database_name, catalogTableName="fact_uber_trips"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(dyf)


job.commit()
