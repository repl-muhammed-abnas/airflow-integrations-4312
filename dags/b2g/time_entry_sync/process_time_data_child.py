import rail
from b2g.time_entry_sync.utils import request_payload, response_filter
null = None

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"b2g_time_data_process_each_record_{config.instance}",
        description=f"b2g TimeSync Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.mandatory_fields_check,
            yes_task="search_user",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            message='\
                {%- if dag_run.conf.Entry_Date | is_falsy -%} \
                    Entry date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.User_Name | is_falsy -%} \
                    User name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Hours | is_falsy -%} \
                    Hours is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Project_Name | is_falsy -%} \
                    Project name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Task_Code | is_falsy -%} \
                    Task code is not present in payload, \
                {%- endif -%}',
            severity='Exception',
            properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': '',
                'status': 'Exception',
            }
        )

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload,
            response_filter=response_filter.get_filtered_user_data
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('search_user')),
            yes_task="get_project_details",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            message="User not available",
            severity='Exception',
            properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': '',
                'status': 'Exception',
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": '{{ dag_run.conf.Project_Name }}',
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                          {"projectDetails": null}])[0]['projectDetails']
        )

        is_project_present = rail.IfOperator(
            task_id="is_project_present",
            test=lambda: bool(rail.result('get_project_details')),
            yes_task="get_task_data",
            no_task="log_project_not_present"
        )

        log_project_not_present = rail.WriteLogOperator(
            task_id='log_project_not_present',
            message="Project not available",
            severity='Exception',
            properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': '',
                'status': 'Exception',
            }
        )

        get_task_data = rail.RepliconServiceOperator(
            task_id='get_task_data',
            endpoint='/services/TaskListService1.svc/GetData',
            data=request_payload.get_task_payload,
            response_filter=response_filter.check_task_data
        )

        has_task_data = rail.IfOperator(
            task_id='has_task_data',
            test=lambda: bool(rail.result("get_task_data")),
            yes_task='check_hours',
            no_task='log_task_not_present'
        )

        log_task_not_present = rail.WriteLogOperator(
            task_id='log_task_not_present',
            message="Task not available",
            severity='Exception',
            properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': '',
                'status': 'Exception',
            }
        )

        check_hours = rail.IfOperator(
            task_id='check_hours',
            test=lambda: bool("{{dag_run.conf.Hours}} > 24"),
            yes_task='get_timsheet_for_date',
            no_task='log_hours_more_than_24'
        )

        log_hours_more_than_24 = rail.WriteLogOperator(
            task_id='log_hours_more_than_24',
            message="Hours is more than 24",
            severity='Exception',
            properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': '',
                'status': 'Exception',
            }
        )

        get_timsheet_for_date = rail.RepliconServiceOperator(
            task_id='get_timsheet_for_date',
            endpoint='/services/TimesheetService1.svc/GetTimesheetForDate2',
            data=request_payload.get_timesheet_for_date
        )

        process_time_entry = rail.RepliconServiceOperator(
            task_id='process_time_entry',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup',
            data=request_payload.get_time_entry_payload
        )

        time_entry_success = rail.WriteLogOperator(
            task_id='time_entry_success',
            message="Time entry was successfully entried in replicon",
            severity='Success',
            properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': "{{result('get_timsheet_for_date').timesheet.uri}}",
                'status': 'Success',

            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
             properties={
                'entrydate': "{{dag_run.conf.Entry_Date}}",
                'username': "{{dag_run.conf.User_Name}}",
                'hours': "{{dag_run.conf.Hours}}",
                'projectname': "{{dag_run.conf.Project_Name}}",
                'taskcode': "{{dag_run.conf.Task_Code}}",
                'comment': "{{dag_run.conf.Comment}}",
                'timesheeturi': '',
                'status': 'failed',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        has_mandatory_fields >> rail.Label("Yes") >> search_user >> is_user_present >> rail.Label("Yes") >> log_user_not_present\
            >> catch_and_log_errors

        has_mandatory_fields >> rail.Label("No") >> log_madatory_fields_not_present >> catch_and_log_errors

        is_user_present >> rail.Label("No") >> get_project_details >> is_project_present\
            >> rail.Label("No") >> log_project_not_present >> catch_and_log_errors
        is_project_present >> rail.Label("Yes") >> get_task_data >> has_task_data

        has_task_data >> rail.Label("Yes") >> check_hours >> rail.Label("Yes") >> log_hours_more_than_24 >> catch_and_log_errors

        check_hours >> rail.Label("No") >> get_timsheet_for_date >> process_time_entry >> time_entry_success >> catch_and_log_errors\
            >> log_to_sumo
        has_task_data >> rail.Label("No") >> log_task_not_present >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
