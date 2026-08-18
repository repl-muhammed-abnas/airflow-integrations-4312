import itertools
import json
import rail
from airflow.models import Variable


def read_collection():
    with rail.lib.readers.get_data_reader(rail.result("query_to_filter_list_to_notify")) as reader:
        create_cloud_clock_datalist = list(reader)
    return create_cloud_clock_datalist


def get_existed_company_in_monitoring_list(config):
    cloudclock_monitoring_alert_list = json.loads(
        Variable.get(config.monitoring_list_var, default_var=[]))

    cloud_clock_datalist = read_collection()

    company_details_list = []

    for company_cloud_data, monitoring_data in itertools.product(cloud_clock_datalist, cloudclock_monitoring_alert_list):
        if company_cloud_data["Company"].lower() == monitoring_data["companykey"].lower():
            existed_company_data = {
                "Company": monitoring_data["companykey"],
                "emailto": monitoring_data["emailto"],
                "Clock": company_cloud_data["Clock"],
                "Last_Update": company_cloud_data["Last_Update"],
                "Unsent_Punches": company_cloud_data["Unsent_Punches"]
            }

            company_details_list.append(existed_company_data)

    return json.dumps(company_details_list)
