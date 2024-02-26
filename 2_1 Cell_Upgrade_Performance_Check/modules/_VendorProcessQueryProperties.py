import re

from modules import _Constants, _Ops
from modules._DataMapModel import _DataMapModel


class _VendorProcessQueryProperties:
    def __init__(self, vendor, filtered_process_type, entity_column, current_group_filter_tag):
        self.__vendor = vendor
        self.__process_type = filtered_process_type
        self.__entity_column = entity_column
        self.__current_group_filter_tag = current_group_filter_tag
        self.__query_feasiability = True
        self.__equiment_segment = str(self.__entity_column) + " AS group_tag"
        self.__data_map_models = []
        self.__process_aggregation_level_list = []
        self.__counter_list = []
        self.__start_datetime_list = []
        self.__end_datetime_list = []
        self.__group_filter_tag_list = []
        self.__vector_process_type_present = False
        self.__counter_ref_id_list = {}
        self.__pre_split_process_type = "(.*)-(VECTOR)-([0-9]+)"
        self.__vector_counter_map = {}
        self.__group_tag_segment = ""
        # print("VendorProcessQueryProperties - self")
        # print(self)
        # self.__process_aggregation_tag = None

        if self.__current_group_filter_tag:
            self.__add_to_group_filter_tag_list(current_group_filter_tag)

    def __add_to_group_filter_tag_list(self, current_group_filter_tag):
        self.__group_filter_tag_list.append(current_group_filter_tag)

    def set_vector_process_type_present(self, vector_process_type_present):
        self.__vector_process_type_present = vector_process_type_present

    def add_to_data_map_models(self, counter_id, process_type, vendor, counter_ref_id):
        data_map_model = _DataMapModel()
        data_map_model.set_counter(counter_id)
        data_map_model.set_process_type(process_type)
        data_map_model.set_vendor(vendor)
        data_map_model.set_time_stamp_id_from_counter_id(counter_id)
        data_map_model.set_counter_ref_id(counter_ref_id)

        self.__data_map_models.append(data_map_model)

    def add_to_process_aggregation_level_list(self, process_aggregation_tag):
        self.__process_aggregation_level_list.append(process_aggregation_tag)

    def add_to_counter_list(self, counter_id):
        self.__counter_list.append(counter_id)

    def add_to_counter_ref_id_list(self, counter_ref_id, counter_id):
        self.__counter_ref_id_list[counter_ref_id] = counter_id

    def add_to_start_datetime_list(self, start_datetime):
        if _Constants.TIMESTAMP_CONVERTION():
            start_datetime = _Ops.convert_timestamp_to_athena_friendly_format(start_datetime)
        self.__start_datetime_list.append(start_datetime)

    def add_to_end_datetime_list(self, end_datetime):
        if _Constants.TIMESTAMP_CONVERTION():
            end_datetime = _Ops.convert_timestamp_to_athena_friendly_format(end_datetime)
        self.__end_datetime_list.append(end_datetime)

    def __get_where_counter_id_in_segment(self):
        return ", ".join(map(str, self.__counter_list))

    def __get_select_counter_segment(self):
        return "counter"

    def __get_where_timestamp_between_segement(self):
        self.__start_datetime_list = list(set(self.__start_datetime_list))
        self.__end_datetime_list = list(set(self.__end_datetime_list))
        if len(self.__start_datetime_list) == 1 and len(self.__end_datetime_list) == 1:
            for start_datetime in self.__start_datetime_list:
                self.__start_datetime = start_datetime
            for end_datetime in self.__end_datetime_list:
                self.__end_datetime = end_datetime
            return ("(timestamp >= '" + str(self.__start_datetime) + "' AND timestamp < '" + str(
                self.__end_datetime) + "')")
        else:
            self.__query_feasiability = False
            return ""

    def __get_select_counter_ref_id_segment(self):
        temp_when_segment_list = []
        for counter_ref_id, counter_id in self.__counter_ref_id_list.items():
            current_when_segment = " WHEN counter = '" + counter_id + "' THEN '" + counter_ref_id + "' "
            temp_when_segment_list.append(current_when_segment)
        temp_case_sgement = " ".join(map(str, temp_when_segment_list))
        return "CASE " + str(temp_case_sgement) + " ELSE 'NO_COUNTER_REF_ID' END"

    def set_tech(self, tech):
        self.__tech = tech

    def __get_group_by_segment(self):
        self.__process_aggregation_level_list = list(set(self.__process_aggregation_level_list))
        # print("self.__process_aggregation_level_list")
        # print(self.__process_aggregation_level_list)
        self.__process_aggregation_level_list = self.__process_aggregation_level_list
        if len(self.__process_aggregation_level_list) == 1:
            for process_aggregation_tag in self.__process_aggregation_level_list:
                self.__process_aggregation_tag = process_aggregation_tag
                self.__group_by_segment = self.__get_group_segment_generator()
            # print("self.__group_by_segment")
            # print(self.__group_by_segment)
            return self.__group_by_segment
        else:
            self.__query_feasiability = False
            return ""

    def __get_group_segment_generator(self):
        group_segment = ""
        # group_tag = "group_tag"
        group_tag = self.__get_select_group_tag_segment()

        if "HOURLY" in self.__process_aggregation_tag:
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(timestamp), MONTH(timestamp), DAY(timestamp), HOUR(timestamp)"
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(year(date_parse(timestamp, '%Y-%m-%d %H-%i'))), MONTH(year(date_parse(timestamp, '%Y-%m-%d %H-%i'))), DAY(year(date_parse(timestamp, '%Y-%m-%d %H-%i'))), HOUR(year(date_parse(timestamp, '%Y-%m-%d %H-%i')))"
            group_segment = " GROUP BY " + str(group_tag) + ", counter, " + self.__get_select_timestamp_segment()
        elif "DAILY" in self.__process_aggregation_tag:
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(timestamp), MONTH(timestamp), DAY(timestamp)"
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(year(date_parse(timestamp, '%Y-%m-%d %H-%i'))), MONTH(year(date_parse(timestamp, '%Y-%m-%d %H-%i'))), DAY(year(date_parse(timestamp, '%Y-%m-%d %H-%i')))"
            group_segment = " GROUP BY " + str(group_tag) + ", counter, " + self.__get_select_timestamp_segment()
        elif "MONTHLY" in self.__process_aggregation_tag:
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(timestamp), MONTH(timestamp)"
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(year(date_parse(timestamp, '%Y-%m-%d %H-%i'))), MONTH(year(date_parse(timestamp, '%Y-%m-%d %H-%i')))"
            group_segment = " GROUP BY " + str(group_tag) + ", counter, " + self.__get_select_timestamp_segment()
        elif "YEARLY" in self.__process_aggregation_tag:
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(timestamp)"
            # group_segment = " GROUP BY " + str(group_tag) + ", counter, YEAR(year(date_parse(timestamp, '%Y-%m-%d %H-%i')))"
            group_segment = " GROUP BY " + str(group_tag) + ", counter, " + self.__get_select_timestamp_segment()
        elif "NOT_MAPPED" in self.__process_aggregation_tag:
            group_segment = " GROUP BY counter"
        elif "NONE-NOT_MAPPED" in self.__process_aggregation_tag:
            group_segment = " GROUP BY " + str(group_tag) + ", counter"

        return group_segment

    def __get_from_segment(self):
        # return Constants.PM_DATABSE() + "." + str(self.__vendor.lower()) + str(self.__tech).lower() + str("parquet")
        # return Constants.PM_DATABSE() + "." + str(Constants.PM_TEST_TABLE())
        if "ericsson" in self.__vendor.lower():
            return _Constants.PM_DATABSE() + "." + str(self.__vendor.lower()) + str(self.__tech).lower() + "15min"
        else:
            return _Constants.PM_DATABSE() + "." + str(self.__vendor.lower()) + str(self.__tech).lower()

    def set_group_filter_type(self, group_filter_type):
        self.__group_filter_type = group_filter_type

    def __get_where_equipment_segment(self):
        where_equipment_segment = ""
        if self.__group_filter_type is "STR":
            if len(self.__group_filter_tag_list) >= 1:
                temp_equipment_segment = []
                for group_filter_tag in self.__group_filter_tag_list:
                    temp_equipment_segment.append(self.__entity_column + " LIKE '%" + group_filter_tag + "%'")
                where_equipment_segment = "AND (" + " AND ".join(map(str, temp_equipment_segment)) + ")"
            else:
                for group_filter_tag in self.__group_filter_tag_list:
                    where_equipment_segment = "AND " + self.__entity_column + " LIKE '%" + group_filter_tag + "%'"
        elif "REGEX" in self.__group_filter_type:
            if len(self.__group_filter_tag_list) == 1:
                for group_filter_tag in self.__group_filter_tag_list:
                    self.__set_group_filter_tag(group_filter_tag)
                else:
                    self.__query_feasiability = False
            where_equipment_segment = "AND REGEXP_LIKE(" + self.__entity_column + ", '" + self.__group_filter_tag + "')"
        elif "NONE" in self.__group_filter_type:
            where_equipment_segment = ""

        # print("where_equipment_segment")
        # print(where_equipment_segment)

        return where_equipment_segment

    def __set_group_filter_tag(self, group_filter_tag):
        self.__group_filter_tag = group_filter_tag

    def get_final_sql_query(self):
        self.__get_where_equipment_segment()
        self.__get_group_by_segment()
        self.__get_select_timestamp_segment()
        self.__get_select_group_tag_segment()
        self.__get_select_counter_segment()
        self.__get_select_process_type_segment()
        self.__get_select_value_segment()
        self.__get_from_segment()
        self.__get_where_timestamp_between_segement()
        self.__get_where_counter_id_in_segment()
        self.__get_select_counter_ref_id_segment()
        self.__get_vendor()

        return "SELECT " + self.__get_select_group_tag_segment() + " AS group_tag, " + self.__get_select_counter_segment() + " AS counter, " + \
               self.__get_select_process_type_segment() + " AS process_type, '" + self.__get_vendor() + "' AS vendor, " + \
               self.__get_select_counter_ref_id_segment() + " AS counter_ref_id, " + self.__get_select_value_segment() + " AS value, " + \
               self.__get_select_timestamp_segment() + " AS datetime FROM " + self.__get_from_segment() + " WHERE " + \
               self.__get_where_timestamp_between_segement() + " AND counter IN (" + self.__get_where_counter_id_in_segment() + ") " + \
               self.__get_where_equipment_segment() + " " + self.__get_group_by_segment()

    def __get_vendor(self):
        return self.__vendor

    def __get_select_group_tag_segment(self):
        if re.match("(NONE)", self.__group_filter_type) is not None:
            return "'All_Network'"
        elif re.match("(NONE_REGEX)", self.__group_filter_type) is not None:
            return "'All_Network'"
        elif re.match("(REGEX_EXTRACT)", self.__group_filter_type) is not None:
            return "REGEXP_EXTRACT(" + self.__entity_column + ", '" + self.__group_filter_tag + "', 0)"
        elif re.match("(REGEX)", self.__group_filter_type) is not None:
            return self.__entity_column
        elif re.match("(STR)", self.__group_filter_type) is not None:
            return self.__entity_column
        else:
            return ""

    def __get_select_timestamp_segment(self):
        select_timestamp_segment = ""

        # datetime = "DATE_FORMAT(CONCAT(YEAR(timestamp), '-', MONTH(timestamp), '-', DAY(timestamp), ' ', HOUR(timestamp), ':', MINUTE(timestamp), ':', SECOND(timestamp)), 'yyyy-MM-dd HH:mm:ss')"
        # datetime_hourly = "DATE_FORMAT(CONCAT(YEAR(timestamp), '-', MONTH(timestamp), '-', DAY(timestamp), ' ', HOUR(timestamp), ':00:00'), 'yyyy-MM-dd HH:mm:ss')"
        # datetime_daily = "DATE_FORMAT(CONCAT(YEAR(timestamp), '-', MONTH(timestamp), '-', DAY(timestamp), ' 00:00:00'), 'yyyy-MM-dd HH:mm:ss')"
        # datetime_monthly = "DATE_FORMAT(CONCAT(YEAR(timestamp), '-', MONTH(timestamp), '-00 00:00:00', 'yyyy-MM-dd HH:mm:ss'))"
        # datetime_yearly = "DATE_FORMAT(CONCAT(YEAR(timestamp), '-00-00 00:00:00')"

        datetime = "concat(cast(year(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(month(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(day(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), ' ', cast(hour(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), ':', cast(minute(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), ':', cast(second(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar))"
        datetime_hourly = "concat(cast(year(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(month(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(day(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), ' ', cast(hour(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), ':0:0')"
        datetime_daily = "concat(cast(year(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(month(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(day(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), ' 0:0:0')"
        datetime_monthly = "concat(cast(year(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-', cast(month(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-1 0:0:0')"
        datetime_yearly = "concat(cast(year(date_parse(timestamp, '%Y-%m-%d %H-%i')) AS varchar), '-1-1 0:0:0')"

        if (
                ("NONE" in self.__group_filter_type or "NONE" in self.__process_aggregation_tag)
                and "HOURLY" is not self.__process_aggregation_tag
                and "DAILY" is not self.__process_aggregation_tag
                and "MONTHLY" is not self.__process_aggregation_tag
                and "YEARLY" is not self.__process_aggregation_tag
        ):
            datetime = "'" + self.__start_datetime + "'"
            datetime_hourly = "'" + self.__start_datetime + "'"
            datetime_daily = "'" + self.__start_datetime + "'"
            datetime_monthly = "'" + self.__start_datetime + "'"
            datetime_yearly = "'" + self.__start_datetime + "'"

        if "HOURLY" is self.__process_aggregation_tag:
            select_timestamp_segment = datetime_hourly
        elif "DAILY" is self.__process_aggregation_tag:
            select_timestamp_segment = datetime_daily
        elif "MONTHLY" is self.__process_aggregation_tag:
            select_timestamp_segment = datetime_monthly
        elif "YEARLY" is self.__process_aggregation_tag:
            select_timestamp_segment = datetime_yearly
        elif "NOT_MAPPED" is self.__process_aggregation_tag or "NONE-NOT_MAPPED" is self.__process_aggregation_tag:
            select_timestamp_segment = datetime
        # elif "NONE-NOT_MAPPED" is self.__process_aggregation_tag:
        #     select_timestamp_segment = datetime
        else:
            select_timestamp_segment = ""

        # print("select_timestamp_segment")
        # print(select_timestamp_segment)

        return select_timestamp_segment

    def __get_select_value_segment(self):
        select_value_segment = ""
        value = ""
        # print("self.__vector_process_type_present():\t" + str(self.__vector_process_type_present))
        if not self.__vector_process_type_present:
            # print("Within if not self.__vector_process_type_present")
            value = "value"
            select_value_segment = self.__get_aggregation_filter_selector(value, self.__process_type)
            # print(select_value_segment)
        else:
            # print("Within else of if not self.__vector_process_type_present")
            temp_vector_list = []
            for counter_ref_id, vector_map_details in self.__vector_counter_map.items():
                counter_id = vector_map_details.get_counter_id()
                vector = vector_map_details.get_process_type()

                pre_split_process_type = _Constants.VECTOR_PRE_SPLIT_PROCESS_TYPE()

                vendor_process_type_search = re.search(pre_split_process_type, vector)

                current_process_type = ""
                vector_index = ""

                if vendor_process_type_search:
                    current_process_type = vendor_process_type_search.group(1)
                    vector_index = vendor_process_type_search.group(3)

                substring_segment = ""

                if counter_id in _Constants.ERICSSON_COMPRESSED_COUNTERS():
                    substring_segment = "REGEXP_EXTRACT(value, \"^\\\\d,(\\\\d+,\\\\d+,?)*(" + vector_index + ",(\\\\d+),?)+(\\\\d+,\\\\d+,?)*$\", 3)"
                else:
                    substring_segment = "SUBSTRING_INDEX(SUBSTRING_INDEX(value, ',', " + vector_index + "), ',', -1)"

                aggregation_filtered_select_value_segment_generator = self.__get_aggregation_filter_selector(
                    substring_segment,
                    current_process_type)
                current_when_segment = " WHEN counter = '" + counter_id + "' THEN " + aggregation_filtered_select_value_segment_generator + " "
                temp_vector_list.append(current_when_segment)

            temp_case_sgement = " ".join(map(str, temp_vector_list))

            select_value_segment = "CASE " + temp_case_sgement + " ELSE 0 END"
            # print(select_value_segment)

        return select_value_segment

    def __get_aggregation_filter_selector(self, value, current_process_type):
        select_value_segment = ""
        raw_value_segment = " (CAST (" + value + " AS decimal ))"
        sum_value_segment = " SUM(CAST (" + value + " AS decimal ))"
        max_value_segment = " MAX(CAST (" + value + " AS decimal ))"
        avg_value_segment = " AVG(CAST (" + value + " AS decimal ))"
        count_value_segment = " COUNT(CAST (" + value + " AS decimal ))"

        current_process_type = current_process_type.strip()

        # print("current_process_type:\t'" + str(current_process_type) + "'")

        if current_process_type is "RAW":
            select_value_segment = raw_value_segment
        elif "SUM" in current_process_type:
            select_value_segment = sum_value_segment
        elif "PEAK" in current_process_type:
            select_value_segment = max_value_segment
        elif "AVG" in current_process_type:
            select_value_segment = avg_value_segment
        elif "COUNT" in current_process_type:
            select_value_segment = count_value_segment
        else:
            select_value_segment = value

        # print("select_value_segment:\t" + str(select_value_segment))

        return select_value_segment

    def __get_select_process_type_segment(self):
        select_process_type_segment = ""
        if not self.__vector_process_type_present:
            select_process_type_segment = "'" + str(self.__process_type) + "'"
        else:
            select_process_type_segment = "'" + str(self.__process_type) + "'"
        return select_process_type_segment

    class VectorMapDetails:
        def __init__(self, counter_id, counter_ref_id, process_type):
            self.__counter_id = counter_id
            self.__counter_ref_id = counter_ref_id
            self.__process_type = process_type

        def get_process_type(self):
            return self.__process_type

        def get_counter_id(self):
            return self.__counter_id

        def get_counter_ref_id(self):
            return self.__counter_ref_id

    def add_to_vector_counter_map(self, counter_id, process_type, counter_ref_id):
        vector_map_details = self.VectorMapDetails(counter_id, counter_ref_id, process_type)

        self.__vector_counter_map[counter_ref_id] = vector_map_details

    def init_all(self):
        self.__vendor
        self.__process_type
        self.__tech
        self.__group_filter_type
        self.__query_feasiability
        self.__vector_process_type_present
        self.__pre_split_process_type
        self.__start_datetime
        self.__end_datetime
        self.__equiment_segment
        self.__group_by_segment
        self.__data_map_models
        self.__process_aggregation_level_list
        self.__counter_list
        self.__start_datetime_list
        self.__end_datetime_list
        self.__group_filter_tag_list
        self.__group_tag_segment
        self.__group_filter_tag
        self.__process_aggregation_tag
        self.__vector_counter_map
        self.__get_where_equipment_segment()
        self.__get_group_by_segment()
        self.__get_select_timestamp_segment()
        self.__get_select_group_tag_segment()
        self.__get_select_counter_segment()
        self.__get_select_process_type_segment()
        self.__get_select_value_segment()
        self.__get_from_segment()
        self.__get_where_timestamp_between_segement()
        self.__get_where_counter_id_in_segment()
        self.__get_select_counter_ref_id_segment()
        self.__get_vendor()

    def print_all(self):

        print("vendor\t" + str(self.__vendor))
        print("processType\t" + str(self.__process_type))
        print("tech\t" + str(self.__tech))
        print("groupFilterType\t" + str(self.__group_filter_type))
        print("queryFeasiability\t" + str(self.__query_feasiability))
        print("vectorProcessTypePresent\t" + str(self.__vector_process_type_present))
        print("preSplitProcessType\t" + str(self.__pre_split_process_type))
        print("startDateTime\t" + str(self.__start_datetime))
        print("endDateTime\t" + str(self.__end_datetime))
        print("equimentSegment\t" + str(self.__equiment_segment))
        print("groupBySegment\t" + str(self.__group_by_segment))
        print("dataMapModels\t" + str(self.__data_map_models))
        print("processAggregationLevelList\t" + str(self.__process_aggregation_level_list))
        print("counterList\t" + str(self.__counter_list))
        print("startDatetimeList\t" + str(self.__start_datetime_list))
        print("endDatetimeList\t" + str(self.__end_datetime_list))
        print("groupFilterTagList\t" + str(self.__group_filter_tag_list))
        print("groupTagSegment\t" + str(self.__group_tag_segment))
        print("groupFilterTag\t" + str(self.__group_filter_tag))
        print("processAggregationTag\t" + str(self.__process_aggregation_tag))
        print("vectorCounterMap\t" + str(self.__vector_counter_map))

        print("self.__get_where_equipment_segment():\t" + str(self.__get_where_equipment_segment()))
        print("self.__get_group_by_segment():\t" + str(self.__get_group_by_segment()))
        print("self.__get_select_timestamp_segment():\t" + str(self.__get_select_timestamp_segment()))
        print("self.__get_select_group_tag_segment():\t" + str(self.__get_select_group_tag_segment()))
        print("self.__get_select_counter_segment():\t" + str(self.__get_select_counter_segment()))
        print("self.__get_select_process_type_segment():\t" + str(self.__get_select_process_type_segment()))
        print("self.__get_select_value_segment():\t" + str(self.__get_select_value_segment()))
        print("self.__get_from_segment():\t" + str(self.__get_from_segment()))
        print("self.__get_where_timestamp_between_segement():\t" + str(self.__get_where_timestamp_between_segement()))
        print("self.__get_where_counter_id_in_segment():\t" + str(self.__get_where_counter_id_in_segment()))
        print("self.__get_select_counter_ref_id_segment():\t" + str(self.__get_select_counter_ref_id_segment()))
        print("self.__get_vendor():\t" + str(self.__get_vendor()))
