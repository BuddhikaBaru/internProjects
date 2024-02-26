# from psutil import virtual_memory
import json
import os


def create_redirect_response(redirect_id):
    redirect_array_list = []
    redirect_map = {}

    redirect_map["redirect_id"] = redirect_id

    redirect_array_list.append(redirect_map)

    return "{" + str(redirect_array_list) + "}"


def convert_timestamp_to_athena_friendly_format(timestamp):
    formated_timestamp = timestamp.replace(":", "-")[:-3]
    return formated_timestamp


def get_available_mem():
    # mem = virtual_memory()
    mem = 5 * 1024 * 1024
    try:
        mem = int(os.environ['json_max_size']) * 1024 * 1024
        # print("json_max_size:\t" + str(mem))
    except:
        print("json_max_size:\tNOT SET")
        pass
    # avalable_mem = mem.total * 0.70

    avalable_mem = mem * 0.90
    return avalable_mem


def get_athena_max_page_size():
    athena_max_page_size = 0
    try:
        athena_max_page_size = int(os.environ['athena_max_page_size'])
        # print("athena_max_page_size:\t" + str(athena_max_page_size))
    except:
        print("athena_max_page_size:\tNOT SET")
        pass
    return athena_max_page_size


def cal_page_size(avg_count):
    max_items_per_page = round(int(get_available_mem() / (50 * avg_count)))
    return max_items_per_page


def format_api_response(result):
    response = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(result),
        "isBase64Encoded": False
    }

    return response
