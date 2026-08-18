import rail

null = None

def get_update_billing_rate_amount_payload(dag_run):
    return {
        "billingRateUri": rail.result("get_company_billing_rate_details")["uri"],
            "rate": {
                "amount": dag_run.conf["item"]["billrate"],
                "currencyUri": rail.result("get_company_billing_rate_details")["effectiveBillingRate"]["value"]["currency"]["uri"]
            },
            "duration": null
        }

def get_add_billing_rate_payload(dag_run):
    return {
        "billingRate": {
            "target": {
              "uri": null,
              "name": dag_run.conf["item"]["Billingratename"]
            },
            "name": dag_run.conf["item"]["Billingratename"],
            "description": dag_run.conf["item"]["description"],
            "isEnabled": "true",
            "rateSchedule": {
                "initialRate": {
                    "amount": dag_run.conf["item"]["billrate"],
                    "currencyUri": rail.result("get_currency_uri")
                }
            }
        }
    }
