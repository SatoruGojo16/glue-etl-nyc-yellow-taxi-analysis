import boto3 as b
from datetime import datetime


def lambda_handler(event, context):
    glue_client = b.client("glue")
    processed_year = datetime.today().year
    processed_month = datetime.today().month
    try:
        glue_client.start_job_run(
            JobName="etl-glue-load-to-postgres",
            Arguments={
                "--PROCESSED_YEAR": str(processed_year),
                "--PROCESSED_MONTH": str(processed_month),
                "--extra-jars": "s3://aws-glue-extras/postgresql-42.7.7.jar",
            },
        )
        return {
            "statusCode": 200,
            "JobName": "etl-glue-load-to-postgres",
            "processed_year": processed_year,
            "processed_month": processed_month,
        }
    except Exception as e:
        print(e)
        raise e
