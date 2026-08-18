"""
Request payload builders for T-Systems Project Billing Rate Import integration.

This module contains functions to build API request payloads for various Replicon
services used in the billing rate assignment process.
"""
import rail
from datetime import datetime

null = None


def get_add_update_billing_rate_conf(payload_data, operation):
    """
    Constructs the configuration for the Add/Update Billing Rate DAG.

    Returns:
        dict: Configuration dictionary for the Add/Update Billing Rate DAG.
    """
    conf_payload = {
        **payload_data,
        'Billing_Rate_Name': payload_data['final_billing_rate_name'],
        'default_currency_uri': payload_data['default_currency_uri'],
        'log': rail.result('create_log'),
        'run_date_time': payload_data['log_job_start_time'],
        'operation_type': operation,
    }

    if operation == 'Update':
        conf_payload.update({
            'billing_rate_uri': payload_data['existing_billing_rate_uri'],
            'existing_billing_rate_amount': payload_data['existing_billing_rate_amount'],
            'existing_billing_rate_name': payload_data['existing_billing_rate_name']
        })

    return conf_payload


def get_add_billing_rate_payload(dag_run):
    return {
        "billingRate": {
            "target": {
                "uri": null,
                "name": dag_run.conf["Billing_Rate_Name"]
            },
            "name": dag_run.conf["Billing_Rate_Name"],
            "description": dag_run.conf["Billing_Rate_ID"],
            "isEnabled": "true",
            "rateSchedule": {
                "initialRate": {
                    "amount": dag_run.conf["Billing_Rate_Value"],
                    "currencyUri": dag_run.conf["default_currency_uri"]
                }
            }
        }
    }


def get_assign_billing_rate_payload(dag_run):
    return {
        "projectUri": rail.result('get_project_details')["project_uri"],
        "billingRateUri": dag_run.conf["billing_rate_uri"],
        "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available",
        "billingRateCopyOptionUri": "urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client"
    }


def get_assign_billing_rate_to_resource_payload(dag_run):
    return {
        "projectUri": rail.result('get_project_details')['project_uri'],
        "teamMembersBillingRates": [
            {
                "resourceUri": rail.result('get_user_uri'),
                "billingRateUrisToAssign": [dag_run.conf['billing_rate_uri']],
                "billingRateUrisToUnassign": []
            }
        ]
    }
