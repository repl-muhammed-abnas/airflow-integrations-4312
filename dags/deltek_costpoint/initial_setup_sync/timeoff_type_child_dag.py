from datetime import timedelta
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timeoff_type_child_{config.instance}',
        description=f'deltek_costpoint_timeoff_type_child_{config.instance}',
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
            no_task='get_modified_leavetype_from_costpoint'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_modified_leavetype_from_costpoint',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_timeofftype_from_costpoint(task_name):
            company_timeofftype_obj = rail.result(
                task_name) if rail.result(task_name) else None
            modified_timeofftypes = []
            for company_paycodes in company_timeofftype_obj:
                if company_paycodes['document']['rows']:
                    for timeofftype in company_paycodes['document']['rows']:
                        if (len(modified_timeofftypes) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_timeofftypes,
                                                                     "timeofftype_code", timeofftype['row']['data'].get(
                                                                         'LV_TYPE_CD'),
                                                                     "timeofftype_name", None) is None):
                            if rail.find_first_by_attr_and_get_attr(modified_timeofftypes,
                                                                    "timeofftype_name", timeofftype['row']['data'].get(
                                                                        'LV_TYPE_DESC'),
                                                                    "timeofftype_name", None):
                                modified_timeofftypes.append({
                                    "timeofftype_name": timeofftype['row']['data'].get('LV_TYPE_DESC')
                                    + "_" +
                                    timeofftype['row']['data'].get(
                                        'LV_TYPE_CD'),
                                    "timeofftype_code": timeofftype['row']['data'].get('LV_TYPE_CD')
                                })
                            else:
                                modified_timeofftypes.append({
                                    "timeofftype_name": timeofftype['row']['data'].get('LV_TYPE_DESC'),
                                    "timeofftype_code": timeofftype['row']['data'].get('LV_TYPE_CD')
                                })

            return modified_timeofftypes

        get_modified_leavetype_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_leavetype_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "replicon_exp_leavetype",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMLVTP_LVTYPE_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "LDMLVTP_LVTYPE_HDR_LAST_MODIFIED",
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

        get_modified_timeofftypes = rail.PythonOperator(
            task_id='get_modified_timeofftypes',
            python_callable=lambda: get_timeofftype_from_costpoint(
                'get_modified_leavetype_from_costpoint')
        )

        def filter_timeoff_types(response):
            return list(map(lambda row:
                            {
                                "name": row["cells"][0]["textValue"],
                                "code": row["cells"][1].get('textValue'),
                                "uri": row["cells"][0]["uri"],
                                "is_enabled": row['cells'][2]['textValue']
                            }, response.json()["d"]["rows"]))

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:time-off-type-list-column:name",
                    "urn:replicon:time-off-type-list-column:description",
                    "urn:replicon:time-off-type-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=filter_timeoff_types
        )

        foreach_timeofftype_flow = rail.ForEachOperator(
            task_id='foreach_timeofftype_flow',
            items="{{ result('get_modified_timeofftypes') | to_json }}",
            start_task='add_update_timeoff',
            end_task='foreach_timeofftype_flow_end'
        )

        def get_timeoff_target_request():
            existing_timeofftypes = rail.result('get_alltimeoff_types')
            timeoff_type_inof = list(
                filter(lambda x: x['code'] == rail.result('foreach_timeofftype_flow')['timeofftype_code'], existing_timeofftypes))
            if timeoff_type_inof and timeoff_type_inof[0]['uri']:
                return {
                    "uri": timeoff_type_inof[0]['uri']
                }
            return {
                "uri": null,
                "name": get_timeofftype_name(rail.result('foreach_timeofftype_flow')['timeofftype_name'],
                                             rail.result('foreach_timeofftype_flow')['timeofftype_code']),
            }

        def get_timeofftype_name(timeofftype_name, timeofftype_code):
            existing_timeoff_types = rail.result('get_alltimeoff_types')
            timeoff_types_by_name = list(
                filter(lambda x: x['name'] == timeofftype_name and
                       x['code'] != timeofftype_code, existing_timeoff_types))
            return timeofftype_name + "_" + timeofftype_code \
                if timeoff_types_by_name and len(timeoff_types_by_name) > 0 else timeofftype_name

        add_update_timeoff = rail.RepliconServiceOperator(
            task_id='add_update_timeoff',
            endpoint="/services/TimeOffService1.svc/PutTimeOffType",
            data=lambda: {
                "timeOffType": {
                    "target": get_timeoff_target_request(),
                    "name": get_timeofftype_name(rail.result('foreach_timeofftype_flow')['timeofftype_name'],
                                                 rail.result('foreach_timeofftype_flow')['timeofftype_code']),
                    "description": rail.result('foreach_timeofftype_flow')['timeofftype_code'],
                    "enabled": 'true',
                    "bookOnCalendarDays": null,
                    "calendarDayDefaultWorkHours": null,
                    "bookOnHolidays": 'false',
                    "totUnscheduledDaysBookingHours": null,
                    "measurementUnitUri": "urn:replicon:time-off-measurement-unit:hours",
                    "timeOffDisplayFormatUri": "urn:replicon:time-off-measurement-unit:hours",
                    "minimumTimeOffIncrementPolicyUri": "urn:replicon:policy:time-off:minimum-increment:full-day",
                    "timeOffDisplayOptionPolicyUri": null,
                    "timeOffBalanceTrackingOptionUri": "urn:replicon:time-off-balance-tracking-option:track-time-remaining",
                    "startEndTimeSpecificationRequirementUri":
                    "urn:replicon:policy:time-off:start-end-time-specification-requirement:require-start-end-time-for-partial-days",
                    "payCodeUri": null,
                    "defaultDaysForBookingTimeOff": null
                }
            }
        )

        def get_action():
            existing_timeofftypes = rail.result('get_alltimeoff_types')
            timeoff_type_inof = list(
                filter(lambda x: x['code'] == rail.result('foreach_timeofftype_flow')['timeofftype_code'], existing_timeofftypes))
            return "Update" if timeoff_type_inof else "Add"

        timeoff_logs_add_entry = rail.WriteLogOperator(
            task_id='timeoff_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda: {
                "timeofftype": rail.result('foreach_timeofftype_flow')['timeofftype_name'],
                "action": get_action(),
                "status": "Succeeded",
                "reason": ""
            }
        )

        foreach_timeofftype_flow_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_flow_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "timeofftype",
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
            'No') >> get_modified_leavetype_from_costpoint >> get_modified_timeofftypes >> get_alltimeoff_types >> \
            foreach_timeofftype_flow >> add_update_timeoff >> \
            timeoff_logs_add_entry >> foreach_timeofftype_flow_end
        foreach_timeofftype_flow >> foreach_timeofftype_flow_end >> catch_and_log_error >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
