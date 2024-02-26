class _DataMapModel:
    def __init__(self):
        pass

    def set_vendor(self, vendor):
        self.__vendor = vendor

    def get_vendor(self):
        return self.__vendor

    def set_process_type(self, process_type):
        self.__process_type = process_type

    def get_process_type(self):
        return self.__process_type

    def set_counter_ref_id(self, counter_ref_id):
        self.__counter_ref_id = counter_ref_id

    def get_counter_ref_id(self):
        return self.__counter_ref_id

    def set_time_stamp_id(self, time_stamp_id):
        self.__time_stamp_id = time_stamp_id

    def set_time_stamp_id_from_counter_id(self, counter):
        self.__time_stamp_id = str(counter) + "_timestamp"

    def get_time_stamp_id(self):
        return self.__time_stamp_id

    def set_group_tag(self, group_tag):
        self.__group_tag = group_tag

    def get_group_tag(self):
        return self.__group_tag

    def set_counter(self, counter):
        self.__counter = counter

    def get_counter(self):
        return self.__counter
