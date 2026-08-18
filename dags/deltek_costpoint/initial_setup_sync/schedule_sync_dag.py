from datetime import timedelta
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_schedules_{config.instance}',
        description=f'deltek_costpoint_schedules_{config.instance}',
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
            no_task='get_modified_schedules_from_costpoint'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_modified_schedules_from_costpoint',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_modified_schedules_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_schedules_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "replicon_exp_tmmworkschedule",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "TMMWORKSCHEDULE_HDR",
                                "conditions": [
                                ],
                                "children": [
                                    {
                                        "rsWhere": {
                                            "rsId": "TMMWORKSCHEDULE_DATE",
                                            "conditions": [
                                                {
                                                    "joinWithParent": "N",
                                                    "relations": [
                                                        {
                                                            "name": "TMMWORKSCHEDULE_DATE_LAST_MODIFIED",
                                                            "relation": "gt=",
                                                            "value": dag_run.conf['last_modified']
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
                    ]
                }
            }
        )

        def get_schedule_details(schedules):
            schedule_details = []
            if schedules and schedules[0] and schedules[0]['document'] and schedules[0]['document']['rows']:
                for schedule in schedules[0]['document']['rows']:
                    mon_hrs = tue_hrs = wed_hrs = thurs_hrs = fri_hrs = sat_hrs = sun_hrs = 0.0
                    if schedule['row'] and schedule['row']['rsId'] and schedule['row'].get('children'):
                        for day_schedule in schedule["row"]["children"]:
                            if day_schedule and day_schedule["row"] and day_schedule["row"]["data"] \
                                    and day_schedule["row"]["data"]["SCHEDULE_DT"] and day_schedule["row"]["data"]["SCHEDULE_DT"] == "1901-01-01T00:00:00":
                                day_of_week = day_schedule["row"]["data"]["S_DAY_OF_WEEK_CD"]
                                if day_of_week == "MON":
                                    mon_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                elif day_of_week == "TUE":
                                    tue_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                elif day_of_week == "WED":
                                    wed_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                elif day_of_week == "THU":
                                    thurs_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                elif day_of_week == "FRI":
                                    fri_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                elif day_of_week == "SAT":
                                    sat_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                elif day_of_week == "SUN":
                                    sun_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]
                                else:
                                    mon_hrs = day_schedule["row"]["data"]["STANDARD_HRS"]

                        schedule_details.append({
                            "name": schedule["row"]["data"]["WORK_SCHEDULE_CD"],
                            "code":  schedule["row"]["data"]["WORK_SCHEDULE_DESC"],
                            "mon_hrs": mon_hrs,
                            "tue_hrs": tue_hrs,
                            "wed_hrs": wed_hrs,
                            "thurs_hrs": thurs_hrs,
                            "fri_hrs": fri_hrs,
                            "sat_hrs": sat_hrs,
                            "sun_hrs": sun_hrs
                        })
            return schedule_details

        get_modified_schedules = rail.PythonOperator(
            task_id='get_modified_schedules',
            python_callable=lambda: get_schedule_details(
                rail.result('get_modified_schedules_from_costpoint'))
        )

        if_costpoint_schedule_present = rail.IfOperator(
            task_id='if_costpoint_schedule_present',
            test="{{result('get_modified_schedules') | length > 0 }}",
            yes_task="get_all_schedules",
            no_task="catch_and_log_error",
        )

        if_schedule_exists_in_polaris = rail.IfOperator(
            task_id='if_schedule_exists_in_polaris',
            test=lambda: get_action() == "Update",
            yes_task="create_edit_draft",
            no_task="create_new_draft",
        )

        get_all_schedules = rail.RepliconServiceOperator(
            task_id='get_all_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data={}
        )

        def decimal_hours_to_timespan(decimal_hours):
            # Calculate the total number of seconds
            total_seconds = int(decimal_hours * 3600)

            # Extract hours, minutes, and seconds
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60

            return {
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": "0",
                "microseconds": "0"
            }

        foreach_schedule_flow = rail.ForEachOperator(
            task_id='foreach_schedule_flow',
            items="{{ result('get_modified_schedules') | to_json }}",
            start_task='if_schedule_exists_in_polaris',
            end_task='foreach_schedule_flow_end'
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id='create_new_draft',
            endpoint="/services/OfficeScheduleService1.svc/CreateNewDraft",
            data={}
        )

        def get_schedule_uri():
            existing_schedules = rail.result('get_all_schedules')
            schedule_uri = rail.find_first_by_attr_and_get_attr(
                existing_schedules, "displayText", rail.result('foreach_schedule_flow')['name'], "uri", None)
            return schedule_uri

        create_edit_draft = rail.RepliconServiceOperator(
            task_id='create_edit_draft',
            endpoint="/services/OfficeScheduleService1.svc/CreateEditDraft",
            data=lambda: {
                     "officeScheduleUri": get_schedule_uri()
            }
        )

        update_name = rail.RepliconServiceOperator(
            task_id='update_name',
            endpoint="/services/OfficeScheduleService1.svc/UpdateName",
            data=lambda: {
                "officeScheduleUri": (rail.result("create_new_draft")),
                "name": rail.result('foreach_schedule_flow')["name"]
            }
        )

        update_description = rail.RepliconServiceOperator(
            task_id='update_description',
            endpoint="/services/OfficeScheduleService1.svc/UpdateDescription",
            data=lambda: {
                "officeScheduleUri": (rail.result("create_new_draft")),
                "description": rail.result('foreach_schedule_flow')["code"]
            }
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id='publish_draft',
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data=lambda: {
                "officeScheduleDraftUri": (get_draft_uri())
            }
        )

        put_simple_schedule_pattern = rail.RepliconServiceOperator(
            task_id='put_simple_schedule_pattern',
            endpoint="/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern",
            data=lambda: {
                "officeScheduleUri": (get_draft_uri()),
                "pattern": {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": get_duration("SUN"),
                    "day2WorkDuration": get_duration("MON"),
                    "day3WorkDuration": get_duration("TUE"),
                    "day4WorkDuration": get_duration("WED"),
                    "day5WorkDuration": get_duration("THU"),
                    "day6WorkDuration": get_duration("FRI"),
                    "day7WorkDuration": get_duration("SAT")
                }
            }
        )

        def get_duration(dayOfWeek):
            schedule_detail = rail.result('foreach_schedule_flow')
            hours = None
            if dayOfWeek == "MON":
                hours = schedule_detail["mon_hrs"]
            elif dayOfWeek == "TUE":
                hours = schedule_detail["tue_hrs"]
            elif dayOfWeek == "WED":
                hours = schedule_detail["wed_hrs"]
            elif dayOfWeek == "THU":
                hours = schedule_detail["thurs_hrs"]
            elif dayOfWeek == "FRI":
                hours = schedule_detail["fri_hrs"]
            elif dayOfWeek == "SAT":
                hours = schedule_detail["sat_hrs"]
            elif dayOfWeek == "SUN":
                hours = schedule_detail["sun_hrs"]
            else:
                hours = schedule_detail["mon_hrs"]
            return decimal_hours_to_timespan(hours)

        def get_draft_uri():
            action = get_action()
            return rail.result("create_edit_draft") if action == "Update" else rail.result("create_new_draft")

        def get_action():
            existing_roles = rail.result('get_all_schedules')
            role_info = list(
                filter(lambda x: x['displayText'] == rail.result('foreach_schedule_flow')["name"], existing_roles))
            return "Update" if role_info else "Add"

        schedule_logs_add_entry = rail.WriteLogOperator(
            task_id='schedule_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda: {
                "rolename": rail.result('foreach_schedule_flow')['name'],
                "action": get_action(),
                "status": "Succeeded",
                "reason": ""
            }
        )

        foreach_schedule_flow_end = rail.EmptyOperator(
            task_id='foreach_schedule_flow_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "Roles",
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
            'No') >> get_modified_schedules_from_costpoint >> get_modified_schedules >> if_costpoint_schedule_present

        if_costpoint_schedule_present >> rail.Label(
            'Yes') >> get_all_schedules >> foreach_schedule_flow >> if_schedule_exists_in_polaris

        if_costpoint_schedule_present >> rail.Label(
            'No') >> catch_and_log_error

        if_schedule_exists_in_polaris >> rail.Label(
            'No') >> create_new_draft >> update_name >> update_description >> put_simple_schedule_pattern

        if_schedule_exists_in_polaris >> rail.Label(
            'Yes') >> create_edit_draft >> put_simple_schedule_pattern

        put_simple_schedule_pattern >> publish_draft >> foreach_schedule_flow_end

        foreach_schedule_flow >> foreach_schedule_flow_end

        foreach_schedule_flow_end >> schedule_logs_add_entry >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
