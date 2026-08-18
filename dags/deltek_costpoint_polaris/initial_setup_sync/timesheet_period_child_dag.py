from datetime import timedelta
from datetime import datetime
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timesheet_period_child_{config.instance}',
        description=f'deltek_costpoint_timesheet_period_child_{config.instance}',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_modified_tsperiods_from_costpoint'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_modified_tsperiods_from_costpoint',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_tsperiods_from_costpoint(task_name):
            company_tsperiods_obj = rail.result(
                task_name) if rail.result(task_name) else None
            modified_tsperiods = []
            for company_tsperiods in company_tsperiods_obj:
                if company_tsperiods['document']['rows']:
                    for tsperiod in company_tsperiods['document']['rows']:
                        if (len(modified_tsperiods) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_tsperiods,
                                                                     "ts_period_name", tsperiod['row']['data'].get(
                                                                         'TS_PD_CD'),
                                                                     "ts_period_name", None) is None):
                            if rail.find_first_by_attr_and_get_attr(modified_tsperiods,
                                                                    "ts_period_description", tsperiod['row']['data'].get(
                                                                        'TS_PD_DESC'),
                                                                    "ts_period_name", None):
                                modified_tsperiods.append({
                                    "ts_period_name": tsperiod['row']['data'].get('TS_PD_CD'),
                                    "ts_period_description": tsperiod['row']['data'].get('TS_PD_DESC') + "_" +
                                    tsperiod['row']['data'].get('TS_PD_CD'),
                                    "ts_period_frequency": tsperiod['row']['data'].get('S_PR_FREQ_CD'),
                                    "ts_period_start_day": tsperiod['row']['data'].get('WK_ST_DAY'),
                                    "ts_period_start_date": tsperiod['row']['children'][0]['row']['data']['START_DT']
                                    if tsperiod['row'].get('children') else None
                                })
                            else:
                                modified_tsperiods.append({
                                    "ts_period_name": tsperiod['row']['data'].get('TS_PD_CD'),
                                    "ts_period_description": tsperiod['row']['data'].get('TS_PD_DESC'),
                                    "ts_period_frequency": tsperiod['row']['data'].get('S_PR_FREQ_CD'),
                                    "ts_period_start_day": tsperiod['row']['data'].get('WK_ST_DAY'),
                                    "ts_period_start_date": tsperiod['row']['children'][0]['row']['data']['START_DT']
                                    if tsperiod['row'].get('children') else None
                                })

            return modified_tsperiods

        get_modified_tsperiods_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_tsperiods_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_tsperiod",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMTSPD_TSPD_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "LDMTSPD_TSPD_HDR_LAST_MODIFIED",
                                                "relation": "gt=",
                                                "value": dag_run.conf['last_modified'],
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }

        )

        get_modified_tsperiods = rail.PythonOperator(
            task_id='get_modified_tsperiods',
            python_callable=lambda: get_tsperiods_from_costpoint(
                'get_modified_tsperiods_from_costpoint')
        )

        def filter_tsperiods_list(response):
            return list(map(lambda row:
                            {
                                "uri": row["cells"][0]["uri"],
                                "name": row["cells"][1].get('textValue'),
                                "code": row["cells"][2].get('textValue')
                            }, response.json()["d"]["rows"]))

        get_all_tsperiodlist = rail.RepliconServiceOperator(
            task_id='get_all_tsperiodlist',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period",
                    "urn:replicon:timesheet-period-list-column:name",
                    "urn:replicon:timesheet-period-list-column:description"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=filter_tsperiods_list
        )

        foreach_tsperiod_flow = rail.ForEachOperator(
            task_id='foreach_tsperiod_flow',
            items="{{ result('get_modified_tsperiods') | to_json }}",
            start_task='add_modify_tsperiod_modifications',
            end_task='foreach_tsperiod_flow_end'
        )

        def get_period_type(period_type):
            period_type_uri = "urn:replicon:timesheet-period-duration:weekly"
            if period_type == 'W':
                period_type_uri = "urn:replicon:timesheet-period-duration:weekly"
            if period_type == 'S':
                period_type_uri = "urn:replicon:timesheet-period-duration:semi-monthly"
            if period_type == 'B':
                period_type_uri = "urn:replicon:timesheet-period-duration:bi-weekly"
            if period_type == 'M':
                period_type_uri = "urn:replicon:timesheet-period-duration:monthly"
            return period_type_uri

        def get_day_of_week(week_start_day):
            day_of_week_uri = "urn:replicon:day-of-week:sunday"
            if week_start_day == '1':
                day_of_week_uri = "urn:replicon:day-of-week:sunday"
            if week_start_day == '2':
                day_of_week_uri = "urn:replicon:day-of-week:monday"
            if week_start_day == '3':
                day_of_week_uri = "urn:replicon:day-of-week:tuesday"
            if week_start_day == '4':
                day_of_week_uri = "urn:replicon:day-of-week:wednessday"
            if week_start_day == '5':
                day_of_week_uri = "urn:replicon:day-of-week:thursday"
            if week_start_day == '6':
                day_of_week_uri = "urn:replicon:day-of-week:friday"
            if week_start_day == '7':
                day_of_week_uri = "urn:replicon:day-of-week:saturday"
            return day_of_week_uri

        def get_day_of_semi_month(day):
            semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:1st-and-16th"
            if day == 1:
                semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:1st-and-16th"
            if day == 2:
                semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:2nd-and-17th"
            if day == 3:
                semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:3rd-and-18th"
            if day == 6:
                semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:6th-and-21st"
            if day == 7:
                semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:7th-and-22nd"
            if day == 8:
                semi_month_uri = "urn:replicon:semi-monthly-frequency-start-day-option:8th-and-23rd"
            if day in [4, 5, 9, 10, 11, 12, 13]:
                end_day = 9 + 15
                semi_month_uri = f"urn:replicon:semi-monthly-frequency-start-day-option:{day}th-and-{end_day}th"
            return semi_month_uri

        def get_day_of_month_uri(day):
            semi_month_uri = "urn:replicon:monthly-frequency-start-day-option:1st"
            if day in [1, 21]:
                semi_month_uri = f"urn:replicon:monthly-frequency-start-day-option:{day}st"
            elif day in [2, 22]:
                semi_month_uri = f"urn:replicon:monthly-frequency-start-day-option:{day}nd"
            elif day in [3, 23]:
                semi_month_uri = f"urn:replicon:monthly-frequency-start-day-option:{day}rd"
            else:
                semi_month_uri = f"urn:replicon:monthly-frequency-start-day-option:{day}th"
            return semi_month_uri

        def get_key_values(period_type, reference_date, week_start_day):
            keyValues = []
            keyValues.append({
                "keyUri": "urn:replicon:timesheet-period-well-known-key:timesheet-period-type",
                "value": {
                    "uri": get_period_type(period_type)
                }
            })

            keyValues.append({
                "keyUri": "urn:replicon:timesheet-period-well-known-key:timesheet-generation-months-in-advance",
                "value": {
                    "number": config.generation_months_in_advance,
                }
            })

            keyValues.append({
                "keyUri": "urn:replicon:timesheet-period-well-known-key:day-of-week",
                "value": {
                    "uri": get_day_of_week(week_start_day),
                }
            })

            if period_type.lower() == 'w':
                keyValues.append({
                    "keyUri": "urn:replicon:timesheet-period-well-known-key:weekly-option",
                    "value": {
                        "uri": "urn:replicon:timesheet-period-weekly-option:regular-week"
                    }
                })

            if period_type.lower() == 'b' and reference_date:
                reference_date_obj = datetime.strptime(
                    reference_date, config.costpoint_date_format)
                keyValues.append({
                    "keyUri": "urn:replicon:timesheet-period-well-known-key:bi-weekly-reference-start-date",
                    "value": {
                        "date": {
                            "year": reference_date_obj.year,
                            "month": reference_date_obj.month,
                            "day": reference_date_obj.day
                        }
                    }
                })

            if period_type.lower() == 's' and reference_date:
                reference_date_obj = datetime.strptime(
                    reference_date, config.costpoint_date_format)
                keyValues.append({
                    "keyUri": "urn:replicon:timesheet-period-well-known-key:days-of-month",
                    "value": {
                        "uri": get_day_of_semi_month(reference_date_obj.day)
                    }
                })

            if period_type.lower() == 'm' and reference_date:
                reference_date_obj = datetime.strptime(
                    reference_date, config.costpoint_date_format)
                keyValues.append({
                    "keyUri": "urn:replicon:timesheet-period-well-known-key:day-of-month",
                    "value": {
                        "uri": get_day_of_month_uri(reference_date_obj.day)
                    }
                })

            return keyValues

        def get_timesheet_period_name(tsperiod_name, tsperiod_code):
            existing_tsperiods = rail.result('get_all_tsperiodlist')
            existing_tsperiods_by_name = list(
                filter(lambda x: x['name'] == tsperiod_name and x['code'] != tsperiod_code, existing_tsperiods))
            return tsperiod_name + "_" + tsperiod_code if existing_tsperiods_by_name \
                and len(existing_tsperiods_by_name) > 0 else tsperiod_name

        def get_tsperiod_request():
            existing_tsperiod = rail.result('get_all_tsperiodlist')
            tsperiod_uri = rail.find_first_by_attr_and_get_attr(existing_tsperiod, "code", rail.result(
                'foreach_tsperiod_flow')['ts_period_name'], "uri", None)
            return {
                "timesheetPeriod": {
                    "target": {"uri": tsperiod_uri} if tsperiod_uri else {"uri": null, "name": rail.result('foreach_tsperiod_flow')['ts_period_description']},
                    "name": get_timesheet_period_name(rail.result('foreach_tsperiod_flow')['ts_period_description'],
                                                      rail.result('foreach_tsperiod_flow')['ts_period_name']),
                    "description": rail.result('foreach_tsperiod_flow')['ts_period_name'],
                    "isEnabled": "1",
                    "keyValues": get_key_values(rail.result('foreach_tsperiod_flow')['ts_period_frequency'],
                                                rail.result('foreach_tsperiod_flow')[
                        'ts_period_start_date'],
                        rail.result('foreach_tsperiod_flow')['ts_period_start_day']),
                }
            }

        add_modify_tsperiod_modifications = rail.RepliconServiceOperator(
            task_id='add_modify_tsperiod_modifications',
            endpoint='/services/TimesheetPeriodService2.svc/PutTimesheetPeriod',
            data=get_tsperiod_request
        )

        tsperiod_logs_add_entry = rail.WriteLogOperator(
            task_id='tsperiod_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda: {
                "paycode": rail.result('foreach_tsperiod_flow')['ts_period_name'],
                "action": "Add / Update",
                "status": "Succeeded",
                "reason": ""
            }
        )

        foreach_tsperiod_flow_end = rail.EmptyOperator(
            task_id='foreach_tsperiod_flow_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "paycode",
                "action": "Add / Update",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_modified_tsperiods_from_costpoint >> get_modified_tsperiods >> get_all_tsperiodlist >> \
            foreach_tsperiod_flow >> add_modify_tsperiod_modifications >> \
            tsperiod_logs_add_entry >> foreach_tsperiod_flow_end
        foreach_tsperiod_flow >> foreach_tsperiod_flow_end >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
