import io
import logging
import sys
import time

import boto3
import pandas as pd

# import numpy as np
from modules import _Constants

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
__logger = logging.getLogger()
# __athena_config = {}


# environ["HTTP_PROXY"] = "http://10.58.74.77:8080"
# environ["HTTPS_PROXY"] = "http://10.58.74.77:8080"

def cleanup(bucket, path, athena_config):
    # session = boto3.Session(
    #     aws_access_key_id=str(_Constants.AWS_SERVER_PUBLIC_KEY),
    #     aws_secret_access_key=str(_Constants.AWS_SERVER_SECRET_KEY),
    # )
    # s3 = boto3.resource('s3', config=Config(proxies={'https': '10.58.74.77:8080'}))
    s3_client = boto3.resource('s3',
                        region_name=athena_config["region"],
                        aws_access_key_id=athena_config["access_key"],
                        aws_secret_access_key=athena_config["access_secret"]
                        )
    # s3 = session.resource('s3')
    bucket = s3_client.Bucket(bucket)
    __logger.info(path)
    objects_to_delete = []
    for obj in bucket.objects.filter(Prefix=path):
        objects_to_delete.append({'Key': obj.key})

    __logger.debug("objects_to_delete:  %s", objects_to_delete)

    bucket.delete_objects(
        Delete={
            'Objects': objects_to_delete
        }
    )


def get_df_from_query(query, database, s3_output, athena_config):
    df = pd.DataFrame()

    try:
        __logger.debug("query:  %s", query)
        __logger.debug("database:  %s", database)
        __logger.debug("s3_output:  %s", s3_output)

        # session = boto3.Session(
        #     aws_access_key_id=str(_Constants.AWS_SERVER_PUBLIC_KEY),
        #     aws_secret_access_key=str(_Constants.AWS_SERVER_SECRET_KEY),
        # )

        # boto3_client = boto3.client('athena', region_name="us-east-1", config=Config(proxies={'https': '10.58.74.77:8080'}))
        __logger.debug("__athena_config:  " + str(athena_config))

        boto3_client = boto3.client(
            'athena',
            region_name=athena_config["region"],
            aws_access_key_id=athena_config["access_key"],
            aws_secret_access_key=athena_config["access_secret"]
        )

        session = boto3.Session(
            aws_access_key_id=athena_config["access_key"],
            aws_secret_access_key=athena_config["access_secret"]
        )

        s3_client = session.client(
            's3',
            region_name=athena_config["region"],
            aws_access_key_id=athena_config["access_key"],
            aws_secret_access_key=athena_config["access_secret"]
        )

        start_time = time.time()

        # boto3_client = session.client('athena', region_name="us-east-1")
        retrieve_response = boto3_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': s3_output
            }
        )
        __logger.debug("Athena Response:  %s", retrieve_response)
        athena_query_id = retrieve_response['QueryExecutionId']

        max_execution = _Constants.ATHENA_PAGINATION_MAX_RETRIES()

        __logger.debug("max_execution:  %s", max_execution)

        state = 'QUEUED'

        scanned_volume_gb = None
        athena_cost = None

        query_state_check = "QUEUED"

        __logger.info("Query State:  %s", state)
        while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
            max_execution = max_execution - 1
            retrieve_response = boto3_client.get_query_execution(QueryExecutionId=athena_query_id)
            __logger.debug("response:  " + str(retrieve_response))

            if state != query_state_check:
                __logger.info("Query State:  %s", state)
                query_state_check = state

            if 'QueryExecution' in retrieve_response and \
                    'Status' in retrieve_response['QueryExecution'] and \
                    'State' in retrieve_response['QueryExecution']['Status']:
                state = retrieve_response['QueryExecution']['Status']['State']
                scanned_volume = None
                if state == 'FAILED' or state == 'CANCELLED':
                    __logger.info("Query State:  FAILED")
                    __logger.info("retrieve_response:  %s", retrieve_response)
                    break
                elif state == 'SUCCEEDED':
                    __logger.info("Query State:  SUCCEEDED")
                    athena_scanned_volume = retrieve_response['QueryExecution']['Statistics']['DataScannedInBytes']
                    scanned_volume_gb = int(athena_scanned_volume) / 1024 / 1024 / 1024

                    __logger.debug("retrieve_response: %s", retrieve_response)
                    __logger.info("Scanned Volume :  %s GB", round(scanned_volume_gb, 5))
                    athena_cost = (scanned_volume_gb / 1024) * 5
                    __logger.info("Query Cost:  $%s", round(athena_cost, 5))
                    __logger.info("Query Process Time:  %s seconds" % round((time.time() - start_time), 5))

                    start_time = time.time()

                    # s3 = boto3.client('s3', config=Config(proxies={'https': '10.58.74.77:8080'}))
                    # s3 = boto3.client('s3')
                    obj = s3_client.get_object(Bucket=_Constants.OUTPUT_S3(), Key=str(athena_query_id) + ".csv")
                    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

                    __logger.info("Result Retrieval Time:  %s seconds" % round((time.time() - start_time), 5))

                    break
            time.sleep(0.5)

        cleanup(_Constants.OUTPUT_S3(), athena_query_id, athena_config)

    except Exception as e:
        # rds_interface = RdsConnector()
        # rds_interface.query_failed_state(redirect_id, "Failed", str(e))
        __logger.info("QUERY FAILED")
        __logger.info(e)
        __logger.info("Unexpected error:", sys.exc_info()[0])
        raise

    return df
