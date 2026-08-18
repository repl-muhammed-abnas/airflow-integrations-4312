import rail
from itvdaytime.user_import.utils import request_payload, custom_methods
from itvdaytime.user_import.tasks.supervisor_task import get_supervisor_task


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_create_user_{config.instance}",
        description=f"iTV DayTime User Import Create User {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_create_user_payload
        )

        remove_timeoff_types = rail.RepliconServiceOperator(
            task_id="remove_timeoff_types",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{result('create_user').uri}}",
                "timeOffTypeUris": []
            }
        )

        get_all_timeoffs = rail.PythonOperator(
            task_id="get_all_timeoffs",
            python_callable=lambda dag_run: custom_methods.get_data_from_document(
                dag_run.conf['time_off_details_collection'])
        )

        get_timeoffs_to_assign_from_mapper = rail.PythonOperator(
            task_id="get_timeoffs_to_assign_from_mapper",
            python_callable=lambda dag_run: custom_methods.get_timeoffs_to_assign_from_mapper(
                config, dag_run)
        )

        assign_supervisor_start, assign_supervisor_end = get_supervisor_task(
            user_uri="{{result('create_user').uri}}", is_update_user=False)

        has_any_timeoffs_to_assign = rail.IfOperator(
            task_id="has_any_timeoffs_to_assign",
            test="{{ result('get_timeoffs_to_assign_from_mapper') | is_truthy}}",
            yes_task="get_all_timeoffs",
            no_task=assign_supervisor_start.task_id
        )

        get_timeoff_payload = rail.PythonOperator(
            task_id="get_timeoff_payload",
            python_callable=request_payload.get_put_timeoff_payload
        )

        assign_timeoff_types_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_types_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.get_assign_timeoff_payload
        )

        assign_timeoff_policies = rail.RepliconServiceOperator(
            task_id="assign_timeoff_policies",
            endpoint="/services/TimeOffService1.svc/PutTimeOffPolicyForUser",
            data="{{result('get_timeoff_payload') | to_json}}"
        )

        log_user_created_success = rail.WriteLogOperator(
            task_id="log_user_created_success",
            severity="process",
            message="User added successfully",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}",
                "status": "Success",
                "action": "Add",
                "details": "User added successfully",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{result('create_user')}}",
                "allowed_for_supervisor_processing": "No"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='User partially created; {{ get_error_message() }}',
            properties=lambda dag_run: {
                "employee_number": dag_run.conf['employee_number'],
                "loginname": dag_run.conf['first_name'] + '.' + dag_run.conf['last_name'],
                "status": "Error",
                "action": "Add",
                "details": 'User partially created; {{ get_error_message() }}',
                "line_manager": dag_run.conf['line_manager'],
                "user_uri": rail.result('create_user').get('uri', "") if rail.result('create_user') else "",
                "allowed_for_supervisor_processing": "No"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_user >> remove_timeoff_types >> get_timeoffs_to_assign_from_mapper >> has_any_timeoffs_to_assign

        has_any_timeoffs_to_assign >> rail.Label("Yes") >> get_all_timeoffs >> assign_timeoff_types_to_user\
            >> get_timeoff_payload >> assign_timeoff_policies >> assign_supervisor_start
        has_any_timeoffs_to_assign >> rail.Label(
            "No") >> assign_supervisor_start

        assign_supervisor_end >> log_user_created_success >> rail.Label(
            "On Error") >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
