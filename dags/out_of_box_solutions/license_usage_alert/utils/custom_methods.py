import json
import rail
from airflow.models import Variable


def check_for_threshold_user_count(threshold):
    return list(filter(
        lambda x: x["licensesremaining"] <= float(threshold)
        if x["licensesremaining"] else False, rail.result('get_product_licensing_summary')))


def check_for_threshold_used_percentage(threshold):
    return list(filter(
        lambda x: x["licensesremainingpercentage"] >= float(threshold)
        if x["licensesremainingpercentage"] else False, rail.result('get_product_licensing_summary')))


def get_companykey_threshold(config):
    licenseUsageAlert_list = json.loads(
        Variable.get("licenseUsageAlert_list", default_var=[]))

    if not licenseUsageAlert_list:
        raise Exception(
            "Either one value should be present in the global variable list")

    company_alert_details = rail.find_first_by_attr_and_get_attr(
        licenseUsageAlert_list, "company_key", config.company_key.lower())

    if not company_alert_details:
        raise Exception(
            f"No alert details found for the company key: {config.company_key}")

    return company_alert_details


def check_for_threshold():
    licenseUsageAlert_list = rail.result("get_company_and_threshold_details")
    if licenseUsageAlert_list['thersholdtype']['Usedpercentage']:
        return bool(check_for_threshold_used_percentage(
            licenseUsageAlert_list['thersholdtype']['thresholdvalue']))

    if licenseUsageAlert_list['thersholdtype']['Usercount']:
        return bool(check_for_threshold_user_count(
            licenseUsageAlert_list['thersholdtype']['thresholdvalue']))

    raise Exception(
        f"thresholdtype should be either Used count or Used percentage, given {licenseUsageAlert_list['thersholdtype']}")
