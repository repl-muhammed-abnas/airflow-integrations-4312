import uuid
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pendulum
import rail

null = None

def get_leave_bal_report_filters(dag_run, time_zone):
    datefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "DateRangeFilter", 'uri')
    start_date = (pendulum.now(time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    return [
        {
            "reportFilterUri": datefilter,
            "value": null
        },
        {
            "reportFilterUri": datefilter,
            "value": dag_run.conf["start_date"] if dag_run.conf and dag_run.conf["start_date"] else start_date
        },
        {
            "reportFilterUri": datefilter,
            "value": dag_run.conf["end_date"] if dag_run.conf and dag_run.conf["end_date"] else start_date
        }
    ]

def get_report_parameters(dag_run, time_zone):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": get_leave_bal_report_filters(dag_run, time_zone),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def construct_policyschedule(new_effective_date, dag_run):
    past_policy_lines = []
    past_effective_policy_line_amount = ""
    for item in rail.result("get_existingpolicy_schedule_for_timeoff")["policySetSchedule"]:
        past_effective_date = datetime.strptime(f"{item['effectiveDate']['day']}/{item['effectiveDate']['month']}/{item['effectiveDate']['year']}", "%d/%m/%Y")
        if past_effective_date.date() == new_effective_date.date():
            past_balance_scripts = (rail.find_first_by_attr_and_get_attr(
                item["policySet"]["timeOffBalanceEventScripts"], "script.uri",
                dag_run.conf["starting_balance_set_to_script_uri"], "additionalParameters"
            ))
            past_effective_policy_line_amount = rail.find_first_by_attr_and_get_attr(past_balance_scripts,
                "keyUri", "urn:replicon:script-key:parameter:amount", "value.number")
        else:
            past_policy_lines.append({
    		    "dateRange": {
                    "startDate": {
                        "year": item['effectiveDate']['year'],
                        "month": item['effectiveDate']['month'],
                        "day": item['effectiveDate']['day']
                    },
                    "endDate": null
                },
    		    "item": {
    		    	"description": f"Effective On {item['effectiveDate']['year']}-{item['effectiveDate']['month']}-{item['effectiveDate']['day']}",
    		    	"policySet": json.loads(json.dumps(
                        item["policySet"], ensure_ascii=False).replace('"script"', '"scriptTarget"'))
    		    }
    	    })

    return (float(past_effective_policy_line_amount) if past_effective_policy_line_amount else 0, past_policy_lines)

def get_put_timeoffpolicyentry(dag_run):
    effective_date = datetime.strptime(dag_run.conf["sellback_leaves_details"]["date"], "%b %d, %Y") - relativedelta(day=1, month=1)
    reset_date_month_uri = "urn:replicon:month:november"
    reset_date_day_uri = "urn:replicon:monthly-frequency-start-day-option:30th"
    (past_effective_policy_line_amount, past_policy_lines) = construct_policyschedule(effective_date, dag_run)
    return {
    	"target": {
    		"uri": dag_run.conf["sellback_leaves_details"]["useruri"],
    		"loginName": null,
    		"employeeId": null,
    		"parameterCorrelationId": null
    	},
    	"template": null,
    	"modifications": {
    		"timeOffTypes": [
    			{
    				"modificationOptionUri": "urn:replicon:collection-modification-option:add",
    				"items": [
    					{
    						"timeOffType": {
                                "uri": rail.result("get_credit_to_timeoff"),
                                "name": null
                            },
    						"isTimeOffAllowedAgainstThisTimeOffType": "true",
    						"applyDefaultTimeOffTypePolicy": "false",
    						"defaultTimeOffTypePolicyEffectiveDate": null,
    						"policySchedule": past_policy_lines + [
    							{
    								"dateRange": {
                                        "startDate": {
                                            "year": effective_date.year,
                                            "month": effective_date.month,
                                            "day": effective_date.day
                                        },
                                        "endDate": null
                                    },
    								"item": {
    									"description": f"Effective On {effective_date.year}-{effective_date.month}-{effective_date.day}",
    									"policySet": {
                                        	"timeOffBalanceEventScripts": [
                                        		{
                                        			"scriptTarget": {
                                        				"uri": dag_run.conf["starting_balance_set_to_script_uri"],
                                        			},
                                        			"additionalParameters": [
                                        				{
                                        					"keyUri": "urn:replicon:script-key:parameter:amount",
                                        					"value": {
                                        						"number": (abs(float(dag_run.conf["sellback_leaves_details"]["amount"]))
                                                                    + past_effective_policy_line_amount)
                                        					}
                                        				},
                                                        {
                                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                                            "value": {
                                                                "number": 10
                                                            }
                                                        }
                                        			]
                                        		},
                                                {
                                                    "scriptTarget": {
                                                        "description": "Reset balance once a year",
                                                        "name": "Yearly Reset",
                                                        "uri": dag_run.conf['yearly_reset_script_uri']
                                                    },
                                                    "additionalParameters": [
                                                        {
                                                            "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                                                            "value": {
                                                                "uri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value"
                                                            }
                                                        },
                                                        {
                                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                                            "value": {
                                                                "number": 20
                                                            }
                                                        },
                                                        {
                                                            "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                                                            "value": {
                                                                "number": 0
                                                            }
                                                        },
                                                        {
                                                            "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                                                            "value": {
                                                                "uri": reset_date_day_uri
                                                            }
                                                        },
                                                        {
                                                            "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                                                            "value": {
                                                                "uri": reset_date_month_uri
                                                            }
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
    								}
    							}
    						]
    					}
    				]
    			}
    		]
    	},
    	"userModificationOptionUri": "urn:replicon:user-modification-option:save",
    	"unitOfWorkId": str(uuid.uuid4())
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = rail.result("gather_policy_assignment_logs")

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log:
        {
            **{
                'jobid': log['ecid']
            },
            **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count", val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count", val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))
    rail.set_result(key="exception_record_count", val= len(list(filter(lambda x: x['status'] == 'Exception', final_log_records ))))
    rail.set_result(key="total_record_count", val=rail.result("create_sellback_leaves_collection", key="length"))

    return final_log_records
