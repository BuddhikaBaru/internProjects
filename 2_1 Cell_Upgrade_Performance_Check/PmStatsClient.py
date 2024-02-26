import logging
import re

import pandas as pd

from modules import _QueryGenerator, _QueryModule, _Constants


class PmStatsClient():
    def __init__(self, access_key, access_secret, region="us-east-1", proxy=None):
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
        self.__logger = logging.getLogger()

        # self.__access_key = access_key
        # self.__access_secret = access_secret
        # self.__proxy = proxy
        # self.__region = region

        self.__athena_config = {
            "access_key": access_key,
            "access_secret": access_secret,
            "proxy": proxy,
            "region": region,
        }

    def set_debug_level(self, level):
        """

        :param level: The logger level (INFO/DEBUG)
        :return: None
        """
        self.__logger.setLevel(level.upper())

    def __stats_query_execute(self, counters_array, group_filter_list, group_filter_type, equpement_column, athena_config):
        queries = _QueryGenerator.generate_queries_v2(counters_array, group_filter_list, group_filter_type,
                                                      equpement_column)

        self.__logger.debug("Query List Size:  %s", len(queries))
        self.__logger.debug("Insert query process:  Start")

        query_array = []

        for current_query_properties in queries:
            self.__logger.debug("current_query_properties")
            self.__logger.debug(current_query_properties)
            current_query = current_query_properties.get_final_sql_query()

            query_array.append(current_query)
            current_query_properties.init_all()

        concated_query = " UNION ALL ".join(map(str, query_array))

        self.__logger.debug("concated_query:  %s", concated_query)

        return _QueryModule.get_df_from_query(concated_query, _Constants.PM_DATABSE(), _Constants.OUTPUT_S3_URL(), athena_config)

    def __generate_json(self, start_date, end_date, vendor, tech, counters_array, group_filter_type, group_filter_list):
        generated_json = {}
        counter_json = {}
        counters_array = self.__counters_array_check(counters_array)
        counter_json["query_counters"] = counters_array
        counter_json['start_datetime'] = start_date
        counter_json['end_datetime'] = end_date
        counter_json['vendor'] = vendor
        counter_json['tech'] = tech
        temp_array = []
        temp_array.append(counter_json)
        generated_json["counters"] = temp_array
        generated_json["counters"] = temp_array
        generated_json["group_filter_type"] = group_filter_type
        group_filter_tags = []
        for current_filter in group_filter_list:
            group_filter_dict = {}
            group_filter_dict["group_filter_tag"] = current_filter
            group_filter_tags.append(group_filter_dict)
        generated_json["group_filter"] = group_filter_tags
        return generated_json

    def __counters_array_check(self, counters_array):
        checked_counters_array = []
        for current_counter in counters_array:
            counter = current_counter["counter_id"]
            counter_ref_id = current_counter["counter_ref_id"]
            process_type = current_counter["process_type"]
            counter = self.__check_accepatble_characters(counter, "counter_id")
            counter_ref_id = self.__check_accepatble_characters(counter_ref_id, "counter_ref_id")
            process_type = self.__check_accepatble_characters(process_type, "process_type")
            process_type = self.__check_process_type(process_type)
            checked_counter = {"counter_id": counter, "counter_ref_id": counter_ref_id, "process_type": process_type}
            checked_counters_array.append(checked_counter)
        return checked_counters_array

    def __check_process_type(self, process_type):
        process_type = process_type.upper()
        if "VECTOR" in process_type:
            regex = "^(([HDMY](SUM|AVG|PEAK|COUNT)|(SUM|AVG|PEAK|COUNT|RAW))-VECTOR-[0-9]+)$"
            pattern = re.compile(regex)
            if pattern.match(process_type):
                return process_type
            else:
                self.__logger.info("%s is not an acceptable process_type", process_type)
                raise Exception("%s is not an acceptable process_type", process_type)
        else:
            regex = "^([HDMY](SUM|AVG|PEAK|COUNT)|(SUM|AVG|PEAK|COUNT|RAW))$"
            pattern = re.compile(regex)
            if pattern.match(process_type):
                return process_type
            else:
                self.__logger.info("%s is not an acceptable process_type", process_type)
                raise Exception("%s is not an acceptable process_type", process_type)

    def __check_accepatble_characters(self, str, type):
        regex = r"^([0-9a-zA-Z\.\-\\(\\)]+)$"
        pattern = re.compile(regex)
        if pattern.match(str):
            return str
        else:
            self.__logger.info("%s is not an acceptable %s", str, type)
            raise Exception("%s is not an acceptable %s", str, type)

    def __get_stats_data(self, generated_json, method_type):
        counters = generated_json["counters"]
        group_filter = generated_json["group_filter"]
        group_filter_type = generated_json["group_filter_type"]
        return self.__stats_query_execute(counters, group_filter, group_filter_type, "equipment", self.__athena_config)

    def __rename_columns(self, df):
        df.rename(columns={
            "gt": "group_tag",
            "c": "counter",
            "pt": "process_type",
            "v": "vendor",
            "cr": "counter_ref_id",
            "cv": "value",
            "dt": "datetime",
        }, inplace=True)
        return df

    def __set_missing_columns(self, df, counters_array):
        columns = df.columns.values
        for current_counter_dict in counters_array:
            current_counter = current_counter_dict["counter_ref_id"]
            if current_counter not in columns:
                df[current_counter] = 0.0
        return df

    def __df_format(self, df, counters_array):
        df = self.__rename_columns(df)
        df = df[~df["vendor"].str.contains("vendor")]
        df["value"] = df["value"].astype(float)
        df = pd.pivot_table(
            df,
            values='value',
            index=['group_tag', 'vendor', 'datetime'],
            columns='counter_ref_id'
        ).reset_index()
        df = self.__set_missing_columns(df, counters_array)
        df.fillna(0, inplace=True)
        df = self.__format_datetime(df)
        return df

    def __format_datetime(self, df):
        df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%d %H:%M:%S")
        return df

    def get_stats(self, start_datetime, end_datetime, vendor, tech, counters_array, group_filter_type, group_filter_list):
        generated_json = self.__generate_json(start_datetime, end_datetime, vendor, tech, counters_array,
                                              group_filter_type, group_filter_list)

        final_df = self.__get_stats_data(generated_json, "STATS")
        final_df = self.__df_format(final_df, counters_array)

        return final_df
