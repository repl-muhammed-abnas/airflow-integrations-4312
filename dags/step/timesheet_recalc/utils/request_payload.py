import rail
null = None


def get_process_time_data_records_conf(item):
    return {
        **{k: v if v is not None else '' for k, v in item.items()}
    }


def get_timesheets_payload(dag_run):
    return {
        "timesheets": list(map(lambda item: item['timesheetperioduri'], dag_run.conf['timesheetdetails']))
    }

def get_slug():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result("get_hourly_report_details")["uri"],
                "filterValues": [{
                    "reportFilterUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":report-filter:8a12f2442c6e4ad0b0146a416d274593;timesheetperiodfilter",
                    "value": null
                },
                    {
                    "reportFilterUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":report-filter:8a12f2442c6e4ad0b0146a416d274593;timesheetperiodfilter",
                    "value": rail.result("get_logging_details")["timerange_start_time"]
                },
                    {
                    "reportFilterUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":report-filter:8a12f2442c6e4ad0b0146a416d274593;timesheetperiodfilter",
                    "value": rail.result("get_logging_details")["timerange_end_time"]
                }],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
