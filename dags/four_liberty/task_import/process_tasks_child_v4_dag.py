
from datetime import timedelta
from airflow.models import Variable
import rail
from four_liberty.task_import.utils import request_payload, response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'4liberty_processtaskschildv4_{config.instance}',
        description=f'4liberty _Process tasks - child V4 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_task_create_update_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_task_create_update_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_task_create_update_log = rail.CreateLogOperator(
            task_id='create_task_create_update_log'
        )

        query_list_59 = rail.QueryCollectionOperator(
            task_id='query_list_59',
            # pylint: disable=line-too-long
            query="""SELECT * FROM existingtasklist WHERE taskname LIKE '{{ dag_run.conf.InternalOrder }}%' AND isenabled = True""",
        )

        get_duplicatetasklist_data = rail.PythonOperator(
            task_id='get_duplicatetasklist_data',
            python_callable=request_payload.get_duplicatetasklist,
        )

        if_request_taskuri_present_3 = rail.IfOperator(
            task_id='if_request_taskuri_present_3',
            test='''{{ dag_run.conf.Taskuri | sn | is_truthy }}''',
            yes_task="_adhoc_http_action_4",
            no_task="_adhoc_http_action_17",
        )

        _adhoc_http_action_4 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_4',
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={"taskUri": "{{ dag_run.conf.Taskuri }}"}
        )

        _adhoc_http_action_5 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_5',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_task_update_payload,
        )

        foreach_d_6 = rail.ForEachOperator(
            task_id='foreach_d_6',
            items='{{ result("_adhoc_http_action_4").customFields | to_json }}',
            start_task='if_customfield_displaytext_equals_to_workorderstatus_7',
            end_task='foreach_d_6_end'
        )

        if_customfield_displaytext_equals_to_workorderstatus_7 = rail.IfOperator(
            task_id='if_customfield_displaytext_equals_to_workorderstatus_7',
            # pylint: disable=line-too-long
            test='''{{ result('foreach_d_6').customField.displayText == 'Work Order Status' and result('foreach_d_6').customField.displayText != dag_run.conf.WorkOrderStatus | upper }}''',
            yes_task="if_workorderstatus_upcase_equals_to_closed_8",
            no_task="foreach_d_6_end",
        )

        if_workorderstatus_upcase_equals_to_closed_8 = rail.IfOperator(
            task_id='if_workorderstatus_upcase_equals_to_closed_8',
            test='''{{ dag_run.conf.WorkOrderStatus| upper == 'CLOSED' or dag_run.conf.WorkOrderStatus | upper == 'LOCKED' }}''',
            yes_task="if_enddate_presence_blank_9",
            no_task="if_workorderstatus_upcase_equals_to_open_13",
        )

        if_enddate_presence_blank_9 = rail.IfOperator(
            task_id='if_enddate_presence_blank_9',
            test=lambda: rail.result("_adhoc_http_action_4") and rail.result(
                "_adhoc_http_action_4")['timeEntryDateRange']['endDate'] == null,
            yes_task="_adhoc_http_action_10",
            no_task="_adhoc_http_action_12",
        )

        _adhoc_http_action_10 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_10',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_adhoc_http_action_10_payload

        )

        else_11 = rail.EmptyOperator(
            task_id='else_11',
        )

        _adhoc_http_action_12 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_12',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_adhoc_http_action_12_payload
        )

        if_workorderstatus_upcase_equals_to_open_13 = rail.IfOperator(
            task_id='if_workorderstatus_upcase_equals_to_open_13',
            test='''{{dag_run.conf.WorkOrderStatus | upper =='OPEN' or dag_run.conf.WorkOrderStatus | upper =='TECO' }}''',
            yes_task="_adhoc_http_action_14",
            no_task="foreach_d_6_end",
        )

        _adhoc_http_action_14 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_14',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_adhoc_http_action_14_payload
        )

        foreach_d_6_end = rail.EmptyOperator(
            task_id='foreach_d_6_end',
        )

        four_liberty_task_import_logger_add_entry_15 = rail.WriteLogOperator(
            task_id='four_liberty_task_import_logger_add_entry_15',
            log="{{ result('create_task_create_update_log') }}",
            message="Task updated",
            severity="Success",
            properties={
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.TaskName }}",
                "budgetcodename": "{{ dag_run.conf.Budgetcodename }}",
                "substationworkordername": "{{ dag_run.conf.Substation_WorkOrderName }}",
                "internal": "{{ dag_run.conf.InternalOrder }}",
                "status": "Success",
                "details": "Task updated",
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        _adhoc_http_action_17 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_17',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_adhoc_http_action_17_payload
        )

        if_workorderstatus_upcase_equals_to_closed_18 = rail.IfOperator(
            task_id='if_workorderstatus_upcase_equals_to_closed_18',
            test='''{{ dag_run.conf.WorkOrderStatus | upper == 'CLOSED' or dag_run.conf.WorkOrderStatus | upper == 'LOCKED' }}''',
            yes_task="_adhoc_http_action_19",
            no_task="copy_projectteamassigment_to_task_20",
        )

        _adhoc_http_action_19 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_19',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_adhoc_http_action_19_payload
        )

        copy_projectteamassigment_to_task_20 = rail.EmptyOperator(
            task_id='copy_projectteamassigment_to_task_20',
        )

        get_all_project_team_assignment = rail.RepliconServiceOperator(
            task_id='get_all_project_team_assignment',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMembers',
            data={
                "projectUri": '{{ dag_run.conf.projecturi }}'
            },
            response_filter=response_filter.get_all_team_members_data
        )

        bulk_update_task_team_members = rail.RepliconServiceOperator(
            task_id='bulk_update_task_team_members',
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            data=request_payload.bulk_update_task_team_members_data
        )

        if_request_duplicatetasklist_greater_than_0_21 = rail.IfOperator(
            task_id='if_request_duplicatetasklist_greater_than_0_21',
            test='''{{ result('get_duplicatetasklist_data') | length > 0 }}''',
            yes_task="close_existing_tasks",
            no_task="four_liberty_task_import_logger_add_entry_23",
        )

        close_existing_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='close_existing_tasks',
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            items='{{ result("get_duplicatetasklist_data") | to_json }}',
            data=request_payload.get_adhoc_http_action_22_payload
        )

        four_liberty_task_import_logger_add_entry_23 = rail.WriteLogOperator(
            task_id='four_liberty_task_import_logger_add_entry_23',
            log="{{ result('create_task_create_update_log') }}",
            message="Task - created and project team resources assigned to task",
            severity="Success",
            properties={
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.TaskName }}",
                "budgetcodename": "{{ dag_run.conf.Budgetcodename }}",
                "substationworkordername": "{{ dag_run.conf.Substation_WorkOrderName }}",
                "internal": "{{ dag_run.conf.InternalOrder }}",
                "status": "Success",
                "details": "Task - created and project team resources assigned to task",
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_task_create_update_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.TaskName }}",
                "budgetcodename": "{{ dag_run.conf.Budgetcodename }}",
                "substationworkordername": "{{ dag_run.conf.Substation_WorkOrderName }}",
                "internal": "{{ dag_run.conf.InternalOrder }}",
                "status": "Error",
                "details": config.error_template,
                "parentjobid": "{{ dag_run.conf.parentjobid }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_task_create_update_log >> query_list_59 >> get_duplicatetasklist_data >> if_request_taskuri_present_3

        if_request_taskuri_present_3 >> rail.Label(
            'Yes') >> _adhoc_http_action_4 >> _adhoc_http_action_5 >> foreach_d_6 >> if_customfield_displaytext_equals_to_workorderstatus_7

        if_customfield_displaytext_equals_to_workorderstatus_7 >> rail.Label(
            'Yes') >> if_workorderstatus_upcase_equals_to_closed_8

        if_workorderstatus_upcase_equals_to_closed_8 >> rail.Label(
            'Yes') >> if_enddate_presence_blank_9
        if_enddate_presence_blank_9 >> rail.Label(
            'Yes') >> _adhoc_http_action_10 >> else_11 >> if_workorderstatus_upcase_equals_to_open_13
        if_enddate_presence_blank_9 >> rail.Label(
            'No') >> _adhoc_http_action_12 >> else_11 >> if_workorderstatus_upcase_equals_to_open_13
        if_workorderstatus_upcase_equals_to_closed_8 >> rail.Label(
            'No') >> if_workorderstatus_upcase_equals_to_open_13
        if_workorderstatus_upcase_equals_to_open_13 >> rail.Label(
            'Yes') >> _adhoc_http_action_14 >> foreach_d_6_end
        if_workorderstatus_upcase_equals_to_open_13 >> rail.Label(
            'No') >> foreach_d_6_end

        if_customfield_displaytext_equals_to_workorderstatus_7 >> rail.Label(
            'No') >> foreach_d_6_end
        foreach_d_6 >> foreach_d_6_end >> four_liberty_task_import_logger_add_entry_15 >> finish

        _adhoc_http_action_17 >> if_workorderstatus_upcase_equals_to_closed_18
        if_workorderstatus_upcase_equals_to_closed_18 >> rail.Label(
            'Yes') >> _adhoc_http_action_19 >> copy_projectteamassigment_to_task_20
        if_workorderstatus_upcase_equals_to_closed_18 >> rail.Label(
            'No') >> copy_projectteamassigment_to_task_20 >> get_all_project_team_assignment >> bulk_update_task_team_members \
            >> if_request_duplicatetasklist_greater_than_0_21
        if_request_duplicatetasklist_greater_than_0_21 >> rail.Label(
            'Yes') >> close_existing_tasks >> four_liberty_task_import_logger_add_entry_23
        if_request_duplicatetasklist_greater_than_0_21 >> rail.Label(
            'No') >> four_liberty_task_import_logger_add_entry_23 >> finish
        if_request_taskuri_present_3 >> rail.Label(
            'No') >> _adhoc_http_action_17

        finish >> catch_and_log_errors >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
