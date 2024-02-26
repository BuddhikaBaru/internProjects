import logging
import re

from modules import _Constants
from modules._VendorProcessQueryProperties import _VendorProcessQueryProperties

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
__logger = logging.getLogger()


def generate_queries_v2(counters, group_filter_array, group_filter_type, entity_column):
    query_list = []

    if group_filter_array:
        for current_group_filter_json_element in group_filter_array:
            current_group_filter_tag = current_group_filter_json_element["group_filter_tag"]

            query_list = vendor_process_type_data_mapper(query_list, counters, group_filter_type, entity_column,
                                                         current_group_filter_tag)
    else:
        query_list = vendor_process_type_data_mapper(query_list, counters, group_filter_type, entity_column, "")

    return query_list


def vendor_process_checker(vendor, filtered_process_type):
    pre_split_process_type = "(.*)-(VECTOR)-([0-9]+)"

    filtered_process_type_search = re.search(pre_split_process_type, filtered_process_type, re.IGNORECASE)

    if filtered_process_type_search:
        counter_id = filtered_process_type_search.group(1)
        vector = filtered_process_type_search.group(2)
        index = filtered_process_type_search.group(3)

        return str(vendor) + "-" + str(vector) + "-" + str(index)

    else:
        return str(vendor) + "-" + str(filtered_process_type)


def vendor_process_list_checker(vendor, filtered_process_type, vendor_process_type_data, entity_column,
                                current_group_filter_tag):
    vendor_check = False

    vendor_process = vendor_process_checker(vendor, filtered_process_type)

    for current_process_vendor in vendor_process_type_data.items():
        if vendor_process in current_process_vendor:
            vendor_check = True

    if not vendor_check:
        new_vendor_data = _VendorProcessQueryProperties(vendor, filtered_process_type, entity_column,
                                                        current_group_filter_tag)

        vendor_process_type_data[vendor_process] = new_vendor_data

    return vendor_process_type_data


def process_aggregation_tagger(process_type, group_filter_type):
    vector_pre_split_process_type = _Constants.VECTOR_PRE_SPLIT_PROCESS_TYPE()
    vector_vendor_process_type_search = re.search(vector_pre_split_process_type, process_type)

    tag = ""
    if vector_vendor_process_type_search:
        process_type = vector_vendor_process_type_search.group(1)

    prefix_regex = _Constants.TIME_PREFIX()
    vector_prefix_regex_search = re.search(prefix_regex, process_type)

    if vector_prefix_regex_search:
        prefix = vector_prefix_regex_search.group(1)

        tag = "NOT_MAPPED"

        if prefix is "H":
            tag = "HOURLY"
        elif prefix is "D":
            tag = "DAILY"
        elif prefix is "M":
            tag = "MONTHLY"
        elif prefix is "Y":
            tag = "YEARLY"
        elif prefix is "NOT_MAPPED":
            tag = "NONE-NOT_MAPPED"

    return tag


def vendor_process_type_data_mapper(query_list, counters, group_filter_type, entity_column, current_group_filter_tag):
    vendor_process_type_data = {}

    for current_counters_json_element in counters:
        start_datetime = current_counters_json_element["start_datetime"]
        end_datetime = current_counters_json_element["end_datetime"]
        vendor = current_counters_json_element["vendor"]
        tech = current_counters_json_element["tech"]

        query_counters_array = current_counters_json_element["query_counters"]

        for query_counters_element in query_counters_array:
            counter_ref_id = query_counters_element["counter_ref_id"]
            counter_id = query_counters_element["counter_id"]
            process_type = query_counters_element["process_type"]

            filtered_process_type = process_type

            vendor_process = vendor_process_checker(vendor, filtered_process_type)

            vendor_process_type_data = vendor_process_list_checker(vendor, filtered_process_type,
                                                                   vendor_process_type_data,
                                                                   entity_column, current_group_filter_tag)

            current_vendor_process_data = vendor_process_type_data.get(vendor_process)
            current_vendor_process_data.add_to_data_map_models(counter_id, filtered_process_type, vendor,
                                                               counter_ref_id)

            process_aggregation_tag = process_aggregation_tagger(process_type, group_filter_type)

            vector_pre_split_process_type = _Constants.VECTOR_PRE_SPLIT_PROCESS_TYPE()

            vendor_process_type_search = re.search(vector_pre_split_process_type, process_type)

            if vendor_process_type_search:
                logging.info('process_type: %s', process_type)
                logging.info('vector_pre_split_process_type: %s', vector_pre_split_process_type)
                current_vendor_process_data.set_vector_process_type_present(True)
                current_vendor_process_data.add_to_vector_counter_map(counter_id, process_type, counter_ref_id)

            current_vendor_process_data.add_to_process_aggregation_level_list(process_aggregation_tag)
            current_vendor_process_data.add_to_counter_list("'" + counter_id + "'")
            current_vendor_process_data.add_to_counter_ref_id_list(counter_ref_id, counter_id)
            current_vendor_process_data.add_to_start_datetime_list(start_datetime)
            current_vendor_process_data.add_to_end_datetime_list(end_datetime)
            current_vendor_process_data.set_tech(tech)
            current_vendor_process_data.set_group_filter_type(group_filter_type)

            vendor_process_type_data[vendor_process] = current_vendor_process_data

    for vendor_process, vendor_process_type_data in vendor_process_type_data.items():
        query_list.append(vendor_process_type_data)

    return query_list
