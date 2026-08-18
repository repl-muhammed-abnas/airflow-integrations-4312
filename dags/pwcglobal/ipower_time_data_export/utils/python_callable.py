from pwcglobal.ipower_time_data_export.utils import response_filter


def create_report_list(config):
    list1 = response_filter.create_timesheet_report_list(config)
    list2 = response_filter.create_location_report_list()
    return list1+list2
