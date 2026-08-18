
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'baylorcollegeofmedicine_workflow_to_disable_user_child_{config.instance}',
        description=f'BaylorCollegeOfMedicine_Child_Workflow to disable user_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_user,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_my_actual_user_identity_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_my_actual_user_identity_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_my_actual_user_identity_3 = rail.RepliconServiceOperator(
            task_id='get_my_actual_user_identity_3',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity",
        )

        if_d_loginname_equals_to_dataworkato_servicereceive_requestrequestuserloginname_4 = rail.IfOperator(
            task_id='if_d_loginname_equals_to_dataworkato_servicereceive_requestrequestuserloginname_4',
            test='''{{ result('get_my_actual_user_identity_3').loginName == dag_run.conf.userloginname }}''',
            yes_task="baylorcollegeofmedicine_user_import_logs_add_entry_5",
            no_task="disable_login_7",
        )

        baylorcollegeofmedicine_user_import_logs_add_entry_5 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_add_entry_5',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.userloginname}}",
                "action": "Disable",
                "status": "Skipped",
                "details": "User is used for integration. Hence, cannot be disabled",
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        disable_login_7 = rail.RepliconServiceOperator(
            task_id='disable_login_7',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        baylorcollegeofmedicine_user_import_logs_add_entry_8 = rail.WriteLogOperator(
            task_id='baylorcollegeofmedicine_user_import_logs_add_entry_8',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Success",
            properties={
                "loginname": "{{dag_run.conf.userloginname}}",
                "action": "Disable",
                "status": "Success",
                "details": "User disabled successfully",
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.userimportlogslookup }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.userloginname}}",
                "action": "Disable",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_my_actual_user_identity_3
        get_my_actual_user_identity_3 >> if_d_loginname_equals_to_dataworkato_servicereceive_requestrequestuserloginname_4
        if_d_loginname_equals_to_dataworkato_servicereceive_requestrequestuserloginname_4 >> rail.Label(
            'Yes') >> baylorcollegeofmedicine_user_import_logs_add_entry_5 >> catch_and_log_error
        if_d_loginname_equals_to_dataworkato_servicereceive_requestrequestuserloginname_4 >> rail.Label(
            'No') >> disable_login_7 >> baylorcollegeofmedicine_user_import_logs_add_entry_8 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
