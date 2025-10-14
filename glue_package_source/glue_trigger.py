import os
import json
import boto3

# O boto3 usará a LabRole injetada na Lambda
glue = boto3.client("glue")
GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))

    # 1. Extrai o caminho completo do arquivo que chegou no S3
    s3_info = event["Records"][0]["s3"]
    bucket_name = s3_info["bucket"]["name"]
    object_key = s3_info["object"]["key"]
    s3_input_path = f"s3://{bucket_name}/{object_key}"

    # 2. Inicia o Glue Job, passando o caminho do arquivo (Requisito 4)
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--S3_INPUT_PATH": s3_input_path,  # ⬅️ Argumento para o script Glue
            "--job-bookmark-option": "job-bookmark-disable",  # Força o Glue a processar
        },
    )

    print(f"Glue Job '{GLUE_JOB_NAME}' started with RunId: {response['JobRunId']}")

    return {
        "statusCode": 200,
        "body": json.dumps({"JobRunId": response["JobRunId"], "file": s3_input_path}),
    }
