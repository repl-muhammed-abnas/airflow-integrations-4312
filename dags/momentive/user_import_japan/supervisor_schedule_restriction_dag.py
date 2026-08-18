from datetime import timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import request_payload

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_supervisor_restriction_dag_id,
        description=f'Momentive Supervisor Schedule Restriction Assignment - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='extract_supervisor_user_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='extract_supervisor_user_id',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        extract_supervisor_user_id = rail.PythonOperator(
            task_id='extract_supervisor_user_id',
            python_callable=lambda dag_run: dag_run.conf['supervisoruri'].split(":")[-1]
        )

        generate_report_per_user = rail.RepliconServiceOperator(
            task_id='generate_report_per_user',
            endpoint="/services/ReportService1.svc/GenerateReport",
            data=request_payload.generate_report_per_supervisor_payload
        )
 
        parse_csv_23 = rail.LoadCSVFileOperator(
            task_id='parse_csv_23',
            document="{{ result('generate_report_per_user').payload }}",
        )

        if_report_has_data = rail.IfOperator(
            task_id='if_report_has_data',
            test="{{ result('parse_csv_23') | is_truthy }}",
            yes_task='formated_users_data_to_csv',
            no_task='log_no_user_data_found'
        )

        formated_users_data_to_csv = rail.WriteCSVFileOperator(
            task_id="formated_users_data_to_csv",
            source="{{ result('parse_csv_23') }}",
            header=["useruri", "useremail", "department"],
            row=request_payload.get_formated_user_row
        )

        create_userdetails_list_from_csv = rail.CreateCollectionOperator(
            task_id='create_userdetails_list_from_csv',
            source='{{result("formated_users_data_to_csv")}}',
            name="user_details"
        )

        query_list = rail.QueryCollectionOperator(
            task_id='query_list',
            query="""SELECT * FROM user_details""",
        )

        foreach_supervisor_users = rail.ForEachOperator(
            task_id='foreach_supervisor_users',
            items="{{ result('query_list') }}",
            start_task='get_enabled_department_groups',
            end_task='foreach_supervisor_users_end'
        )

        get_enabled_department_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups',
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
            data_handler= lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", rail.result('foreach_supervisor_users')['department'], "uri")
        )

        if_department_uri_present = rail.IfOperator(
            task_id='if_department_uri_present',
            test="{{ result('get_enabled_department_groups') | is_truthy }}",
            yes_task='update_supervisor_schedule_restriction',
            no_task='foreach_supervisor_users_end'
        )

        update_supervisor_schedule_restriction = rail.RepliconServiceOperator(
            task_id='update_supervisor_schedule_restriction',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=request_payload.restrict_supervisor_schedule_payload
        )

        log_supervisor_restriction_updated = rail.WriteLogOperator(
            task_id='log_supervisor_restriction_updated',
            log=lambda dag_run: dag_run.conf['log'],
            message="na",
            severity="Success",
            properties=lambda: {
                "value": "Supervisor schedule restriction updated",
                "user_uri": rail.result('foreach_supervisor_users')['useruri'],
                "department": rail.result('foreach_supervisor_users')['department']
            }
        )

        foreach_supervisor_users_end = rail.EmptyOperator(
            task_id='foreach_supervisor_users_end',
        )

        log_no_user_data_found = rail.WriteLogOperator(
            task_id='log_no_user_data_found',
            log=lambda dag_run: dag_run.conf['log'],
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "value": "No user data found in supervisor report",
                "supervisor_uri": dag_run.conf['supervisoruri']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log=lambda dag_run: dag_run.conf['log'],
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "value": "Error processing supervisor schedule restrictions",
                "supervisor_uri": "{{ dag_run.conf.supervisoruri }}",
                "error_details": "{{ get_error_message() }}"
            }
        )
        
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> extract_supervisor_user_id

        extract_supervisor_user_id >> generate_report_per_user >> parse_csv_23 >> if_report_has_data

        if_report_has_data >> rail.Label('Yes') >> formated_users_data_to_csv >> create_userdetails_list_from_csv >> query_list >> foreach_supervisor_users

        foreach_supervisor_users >> get_enabled_department_groups >> if_department_uri_present >> rail.Label('Yes') >> update_supervisor_schedule_restriction >> log_supervisor_restriction_updated >> foreach_supervisor_users_end
        if_department_uri_present >> rail.Label('No') >> foreach_supervisor_users_end

        foreach_supervisor_users >> foreach_supervisor_users_end >> catch_and_log_error

        if_report_has_data >> rail.Label('No') >> log_no_user_data_found >> catch_and_log_error


    return dag

rail.for_each_instance(create_dag)
