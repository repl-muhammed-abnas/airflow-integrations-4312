import json
from datetime import datetime

import rail
null = None


def get_additional_time_off_type_policy_payload(dag_run, timeoff_type_to, timeoff_type_from):
    policy = json.loads(json.dumps(rail.result('get_user_timeoff_policysetschedule'))
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
    effective_date = datetime.strptime(dag_run.conf["efective_date_for_new_policyset"], "%Y/%m/%d")
    balance = rail.find_first_by_attr_and_get_attr(dag_run.conf["balance_to_transfer"], 'name', timeoff_type_to, 'balance')
    event_scripts = rail.result('get_all_timeoff_event_scripts')
    validation_scripts = rail.result('get_all_timeoff_validation_scripts')
    policy.extend([
            {
            "effectiveDate": rail.get_replicon_date(effective_date),
            "description": "Effective On "+ dag_run.conf["efective_date_for_new_policyset"],
            "policySet": {
                "timeOffBalanceEventScripts": [
                {
                    "scriptTarget": {
                    "uri": event_scripts['yearly_monthly_accrual_with_expiry_rounding']
                    },
                    "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:accrual-type",
                        "value": {
                        "uri": "urn:replicon:accrual-schedule:yearly"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:yearly-entitlement",
                        "value": {
                        "number": balance
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                        "value": {
                        "uri": "urn:replicon:month:january"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                        "value": {
                        "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:prorate-for-policy-start-and-end",
                        "value": {
                        "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:prorate-for-users-end-date",
                        "value": {
                        "uri": "urn:replicon:prorate-for-user-end-date:do-not-prorate"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:policy-start-proration-exception-months",
                        "value": {
                        "uri": "urn:replicon:no-of-months:no-proration-exception"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:policy-start-proration-exception-days",
                        "value": {
                        "uri": "urn:replicon:no-of-days:0-days"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:user-end-proration-exception-months",
                        "value": {
                        "uri": "urn:replicon:no-of-months:no-proration-exception"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:user-end-proration-exception-days",
                        "value": {
                        "uri": "urn:replicon:no-of-days:0-days"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:round-accrued-balance",
                        "value": {
                        "uri": "urn:replicon:round-up:do-not-round"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:rounding-threshold",
                        "value": {
                        "number": 0
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:expiry-enabled",
                        "value": {
                        "uri": "urn:replicon:expiry-enabled:yes"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:expire-after",
                        "value": {
                        "number": 60
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:expire-after-units",
                        "value": {
                        "uri": "urn:replicon:time-off-expire-after-unit:months"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:expiry-upon-option",
                        "value": {
                        "uri": "urn:replicon:time-off-upon-expiry-option:do-not-pay-out"
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:expiry-paycode-name",
                        "value": {
                        "text": ""
                        }
                    },
                    {
                        "keyUri": "urn:replicon:script-key:parameter:precedence",
                        "value": {
                        "number": "30"
                        }
                    }
                    ]
                }
                ],
                "timeOffValidationScripts": [
                {
                    "scriptTarget": {
                    "uri": validation_scripts['nl_past_booking_restriction']
                    },
                    "additionalParameters": [

                    ]
                },
                {
                    "scriptTarget": {
                    "uri": validation_scripts['prevent_balance_overdraw']
                    },
                    "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                        "value": {
                        "number": "0"
                        }
                    }
                    ]
                },
                {
                    "scriptTarget": {
                    "uri": validation_scripts['require_other_time_off_balance_to_be_used_first']
                    },
                    "additionalParameters": [
                    {
                        "keyUri": "urn:replicon:script-key:parameter:time-off-to-be-used",
                        "value": {
                        "text": timeoff_type_from
                        }
                    }
                    ]
                }
                ]
            }
            }
        ])

    return {
        "timeOffAccount": {
            "userUri": rail.result("get_user_details")["useruri"],
            "timeOffTypeUri": rail.result('for_each_timeofftype')['uri']
        },
        "policySetScheduleEntries": policy
    }

def get_carried_over_time_off_type_policy_payload(dag_run, timeoff_type):
    policy = json.loads(json.dumps(rail.result('get_user_timeoff_policysetschedule'))
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
    effective_date = datetime.strptime(dag_run.conf["efective_date_for_new_policyset"], "%Y/%m/%d")
    balance = rail.find_first_by_attr_and_get_attr(dag_run.conf["balance_to_transfer"], 'name', timeoff_type, 'balance')
    event_scripts = rail.result('get_all_timeoff_event_scripts')
    validation_scripts = rail.result('get_all_timeoff_validation_scripts')
    policy.extend([
            {
            "effectiveDate": rail.get_replicon_date(effective_date),
            "description": "Effective On "+ dag_run.conf["efective_date_for_new_policyset"],
            "policySet": {
              "timeOffBalanceEventScripts": [
                {
                  "scriptTarget": {
                    "uri": event_scripts['yearly_monthly_accrual_with_expiry_rounding']
                  },
                  "additionalParameters": [
                    {
                      "keyUri": "urn:replicon:script-key:parameter:accrual-type",
                      "value": {
                        "uri": "urn:replicon:accrual-schedule:yearly"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:yearly-entitlement",
                      "value": {
                        "number": balance
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                      "value": {
                        "uri": "urn:replicon:month:january"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                      "value": {
                        "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:prorate-for-policy-start-and-end",
                      "value": {
                        "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:prorate-for-users-end-date",
                      "value": {
                        "uri": "urn:replicon:prorate-for-user-end-date:do-not-prorate"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:policy-start-proration-exception-months",
                      "value": {
                        "uri": "urn:replicon:no-of-months:no-proration-exception"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:policy-start-proration-exception-days",
                      "value": {
                        "uri": "urn:replicon:no-of-days:0-days"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:user-end-proration-exception-months",
                      "value": {
                        "uri": "urn:replicon:no-of-months:no-proration-exception"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:user-end-proration-exception-days",
                      "value": {
                        "uri": "urn:replicon:no-of-days:0-days"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:round-accrued-balance",
                      "value": {
                        "uri": "urn:replicon:round-up:do-not-round"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:rounding-threshold",
                      "value": {
                        "number": 0
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:expiry-enabled",
                      "value": {
                        "uri": "urn:replicon:expiry-enabled:yes"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:expire-after",
                      "value": {
                        "number": 6
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:expire-after-units",
                      "value": {
                        "uri": "urn:replicon:time-off-expire-after-unit:months"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:expiry-upon-option",
                      "value": {
                        "uri": "urn:replicon:time-off-upon-expiry-option:do-not-pay-out"
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:expiry-paycode-name",
                      "value": {
                        "text": null
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:precedence",
                      "value": {
                        "number": "30"
                      }
                    }
                  ]
                }
              ],
              "timeOffValidationScripts": [
                {
                  "scriptTarget": {
                    "uri": validation_scripts['prevent_balance_overdraw']
                  },
                  "additionalParameters": [
                    {
                      "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                      "value": {
                        "number": "0"
                      }
                    }
                  ]
                },
                {
                  "scriptTarget": {
                    "uri": validation_scripts['prevent_use_during_probationary_period']
                  },
                  "additionalParameters": [
                    {
                      "keyUri": "urn:replicon:script-key:parameter:probationary-period",
                      "value": {
                        "number": 90
                      }
                    },
                    {
                      "keyUri": "urn:replicon:script-key:parameter:probationary-period-unit",
                      "value": {
                        "uri": "urn:replicon:time-off-expire-after-unit:days"
                      }
                    }
                  ]
                },
                {
                  "scriptTarget": {
                    "uri": validation_scripts['nl_past_booking_restriction']
                  },
                  "additionalParameters": [

                  ]
                }
              ]
            }
            }
        ])

    return {
        "timeOffAccount": {
            "userUri": rail.result("get_user_details")["useruri"],
            "timeOffTypeUri": rail.result('for_each_timeofftype')['uri']
        },
        "policySetScheduleEntries": policy
    }
