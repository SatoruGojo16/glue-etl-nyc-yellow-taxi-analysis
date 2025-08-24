import json
import boto3 as b
import re


def lambda_handler(event, context):
    glue_client = b.client("glue")
    uploaded_file = event["Records"][0]["s3"]
    uploaded_file_bucket = uploaded_file["bucket"]["name"]
    uploaded_file_key = uploaded_file["object"]["key"]
    try:
        file_name = uploaded_file_key.split("/")[-1].split(".")[0]
        processed_year = int(re.findall(r"\d{4}", file_name)[0])
        processed_month = int(re.findall(r"\d{2}$", file_name)[0])
        glue_client.start_job_run(
            JobName="etl-glue-nyc-yellow-data-model",
            Arguments={
                "--PROCESSED_MONTH": str(processed_month),
                "--PROCESSED_YEAR": str(processed_year),
                "--SOURCE_RAW_FILE_PATH": f"s3://{uploaded_file_bucket}/{uploaded_file_key}",
            },
        )
        return {
            "statusCode": 200,
            "uploaded_file_bucket": uploaded_file_bucket,
            "uploaded_file_key": uploaded_file_key,
            "processed_year": processed_year,
            "processed_month": processed_month,
            "SOURCE_RAW_FILE_PATH": f"s3://{uploaded_file_bucket}/{uploaded_file_key}",
        }
    except Exception as e:
        print(e)
        raise e
