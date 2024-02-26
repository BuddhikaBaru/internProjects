def PM_DATABSE():
    return str("countervalues")


def PM_TEST_TABLE():
    return str("testoutput")


def VECTOR_PRE_SPLIT_PROCESS_TYPE():
    return "(.*)-(VECTOR)-([0-9]+)"


def TIME_PREFIX():
    return "^([H|D|M|Y])[SUMPEAKVGCONT]+$"


def OUTPUT_S3_URL():
    return str("s3://" + OUTPUT_S3() + "/")


def OUTPUT_S3():
    return str("athena-results-bucket-01")


def TIMESTAMP_CONVERTION():
    return True


def ERICSSON_COMPRESSED_COUNTERS():
    # ericsson_counters = open("ericssonCompressedCounters.txt", "r")
    # return ericsson_counters.read().split(',')
    return [
        "pmErabQciLevSum",
        "pmErabRelAbnormalEnbActQci",
        "pmErabRelAbnormalMmeActQci",
        "pmErabRelAbnormalEnbQci",
        "pmErabRelNormalEnbQci",
        "pmErabEstabSuccAddedQci",
        "pmErabEstabSuccInitQci",
        "pmErabEstabAttAddedQci",
        "pmErabEstabAttInitQci",
        "pmErabRelMmeQci"
    ]


def RDS_URI():
    return str("pm-query-meta-db.cicclfzj9vuy.ap-southeast-1.rds.amazonaws.com")


def RDS_PASSWORD():
    return str("fUTckGVuznybnqqE")


def RDS_USERNAME():
    return str("lambda_user")


def RDS_PORT():
    return int(3306)


def RDS_DATABASE():
    return str("querylogdb")


def QUERY_TABLE_COLUMNS():
    return ["redirect_id", "query_id", "athena_query_id", "pagination", "no_of_pages", "status", "query_init_time",
            "result_complete_time", "result_return_time", "db_insertion_time", "query_received_time"]


def ATHENA_PAGINATION_MAX_RETRIES():
    return 900


def REGION():
    return str("us-east-1")


def ATHENA_PAGINATION_JSON_SAVE_THRESHOLD():
    return 10


def AWS_SERVER_PUBLIC_KEY():
    return "AKIATDKLDNFXLIEP2GN7"


def AWS_SERVER_SECRET_KEY():
    return "VwuDn38scOGWuX2vNfP3bIt+ORwyegXc57X/zrGR"
