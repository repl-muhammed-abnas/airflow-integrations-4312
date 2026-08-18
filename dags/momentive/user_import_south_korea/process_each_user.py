# pylint: disable=too-many-statements
from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_south_korea.utils import request_payload, python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_proecss_each_user_child_{config.instance}',
        description=f'momentive_userimport_proecss_each_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_each_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_user',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_user_payload,
            data_handler=python_callable.get_user_data
        )

        create_user_log = rail.CreateLogOperator(
            task_id = "create_user_log"
        )

        get_all_req_uri_details_40 = rail.PythonOperator(
            task_id = "get_all_req_uri_details_40",
            python_callable=python_callable.get_req_uris
        )

        if_get_req_useruri_present_45 = rail.IfOperator(
            task_id="if_get_req_useruri_present_45",
            test="{{ result('get_all_req_uri_details_40').useruri | is_truthy }}",
            yes_task="if_userstatus_is_false",
            no_task="if_active_is_1_74"
        )

        if_userstatus_is_false = rail.IfOperator(
            task_id="if_userstatus_is_false",
            test="{{ result('get_all_req_uri_details_40').status.lower() == 'false' }}",
            yes_task="if_active_present_and_0_49",
            no_task="if_userstatus_is_true"
        )

        if_active_present_and_0_49 = rail.IfOperator(
            task_id="if_active_present_and_0_49",
            test="{{ dag_run.conf.active | is_truthy and \
                dag_run.conf.active == '0' }}",
            yes_task="if_enddate_user_present",
            no_task="if_active_present_and_1_64"
        )

        if_enddate_user_present = rail.IfOperator(
            task_id="if_enddate_user_present",
            test="{{ result('get_all_req_uri_details_40').enddate | is_truthy }}",
            yes_task="log_user_disabled_with_enddate",
            no_task="if_terminationdate_present"
        )

        log_user_disabled_with_enddate = rail.WriteLogOperator(
            task_id="log_user_disabled_with_enddate",
            log = '{{ result("create_user_log") }}',
            message = "Exception",
            severity = "Exception",
            properties=request_payload.log_process_user_payload
        )

        if_terminationdate_present = rail.IfOperator(
            task_id="if_terminationdate_present",
            test="{{ dag_run.conf.terminationdate | is_truthy }}",
            yes_task="validate_terminationdate",
            no_task="catch_and_log_error"
        )

        validate_terminationdate = rail.IfOperator(
            task_id="validate_terminationdate",
            test=lambda dag_run: bool((datetime.strptime(
                dag_run.conf['terminationdate'], "%Y-%m-%d")).date() >= datetime.now().date()),
            yes_task="if_terminationdate_less_than_startdate",
            no_task="log_usernotdisabled_63"
        )

        log_usernotdisabled_63 = rail.WriteLogOperator(
            task_id="log_usernotdisabled_63",
            log = '{{ result("create_user_log") }}',
            message = "Exception",
            severity = "Exception",
            properties=request_payload.log_process_user_payload
        )

        if_terminationdate_less_than_startdate = rail.IfOperator(
            task_id="if_terminationdate_less_than_startdate",
            test=lambda dag_run: bool((datetime.strptime(
                dag_run.conf['terminationdate'], "%Y-%m-%d")).date() < datetime.strptime(
                rail.result('get_all_req_uri_details_40')['startdate'], "%Y-%m-%d").date()),
            yes_task="log_user_already_disabled_59",
            no_task="trigger_disbale_user"
        )

        log_user_already_disabled_59 = rail.WriteLogOperator(
            task_id="log_user_already_disabled_59",
            log = '{{ result("create_user_log") }}',
            message = "Exception",
            severity = "Exception",
            properties=request_payload.log_process_user_payload
        )

        if_active_present_and_1_64 = rail.IfOperator(
            task_id="if_active_present_and_1_64",
            test="{{ dag_run.conf.active | is_truthy and \
                dag_run.conf.active == '1' }}",
            yes_task="trigger_update_user",
            no_task="catch_and_log_error"
        )

        if_userstatus_is_true = rail.IfOperator(
            task_id="if_userstatus_is_true",
            test="{{ result('get_all_req_uri_details_40').status.lower() == 'true' }}",
            yes_task="if_active_present_and_0_67",
            no_task="catch_and_log_error"
        )

        if_active_present_and_0_67 = rail.IfOperator(
            task_id="if_active_present_and_0_67",
            test="{{ dag_run.conf.active | is_truthy and \
                dag_run.conf.active == '0' }}",
            yes_task="trigger_disbale_user",
            no_task="if_active_present_and_1_69"
        )

        trigger_disbale_user = rail.TriggerDagRunOperator(
            task_id='trigger_disbale_user',
            trigger_dag_id=f'momentive_userimport_disable_user_child_{config.instance}',
            conf=request_payload.process_disable_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_disable_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_disable_user',
            dag_runs='{{ result("trigger_disbale_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_present_and_1_69 = rail.IfOperator(
            task_id="if_active_present_and_1_69",
            test="{{ dag_run.conf.active | is_truthy and \
                dag_run.conf.active == '1' }}",
            yes_task="trigger_update_user",
            no_task="if_active_notpresent_or_blank_71"
        )

        trigger_update_user = rail.TriggerDagRunOperator(
            task_id='trigger_update_user',
            trigger_dag_id=f'momentive_userimport_user_sync_update_child_{config.instance}',
            conf=request_payload.process_update_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("trigger_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_notpresent_or_blank_71 = rail.IfOperator(
            task_id="if_active_notpresent_or_blank_71",
            test="{{ dag_run.conf.active | is_falsy or \
                dag_run.conf.active == '-' }}",
            yes_task="log_blank_userstatus_72",
            no_task="catch_and_log_error"
        )

        log_blank_userstatus_72 = rail.WriteLogOperator(
            task_id="log_blank_userstatus_72",
            log = '{{ result("create_user_log") }}',
            message = "Exception",
            severity = "Exception",
            properties=request_payload.log_process_user_payload
        )

        if_active_is_1_74 = rail.IfOperator(
            task_id="if_active_is_1_74",
            test="{{ dag_run.conf.active == '1' }}",
            yes_task="trigger_add_user",
            no_task="if_active_0_or_blank_76"
        )

        trigger_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_add_user',
            trigger_dag_id=f'momentive_userimport_user_sync_add_child_{config.instance}',
            conf=request_payload.process_add_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs='{{ result("trigger_add_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_0_or_blank_76 = rail.IfOperator(
            task_id="if_active_0_or_blank_76",
            test="{{ dag_run.conf.active == '0' or dag_run.conf.active == '-' }}",
            yes_task="log_user_disabled_in_workday_77",
            no_task="catch_and_log_error"
        )

        log_user_disabled_in_workday_77 = rail.WriteLogOperator(
            task_id="log_user_disabled_in_workday_77",
            log = '{{ result("create_user_log") }}',
            message = "Exception",
            severity = "Exception",
            properties=request_payload.log_process_user_payload
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ result("create_user_log") }}',
            trigger_rule='one_failed',
            message="Error",
            severity="Error",
            properties={
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "action": "Process user",
                "status": "Error",
                'details': "{{ get_error_message() }}",
                'country':''
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> search_user

        search_user >> create_user_log >> get_all_req_uri_details_40 >> if_get_req_useruri_present_45

        if_get_req_useruri_present_45 >> rail.Label('Yes') >> if_userstatus_is_false
        if_get_req_useruri_present_45 >> rail.Label('No') >> if_active_is_1_74

        if_userstatus_is_false >> rail.Label('Yes') >> if_active_present_and_0_49
        if_userstatus_is_false >> rail.Label('No') >> if_userstatus_is_true

        if_active_present_and_0_49 >> rail.Label('Yes') >> if_enddate_user_present
        if_active_present_and_0_49 >> rail.Label('No') >> if_active_present_and_1_64

        if_enddate_user_present >> rail.Label('Yes') >> log_user_disabled_with_enddate
        if_enddate_user_present >> rail.Label('No') >> if_terminationdate_present

        if_terminationdate_present >> rail.Label('Yes') >> validate_terminationdate
        if_terminationdate_present >> rail.Label('No') >> catch_and_log_error

        validate_terminationdate >> rail.Label('Yes') >> if_terminationdate_less_than_startdate
        validate_terminationdate >> rail.Label('No') >> log_usernotdisabled_63 >> catch_and_log_error

        if_terminationdate_less_than_startdate >> rail.Label('Yes') >> log_user_already_disabled_59 >> catch_and_log_error
        if_terminationdate_less_than_startdate >> rail.Label('No') >> trigger_disbale_user

        if_active_present_and_1_64 >> rail.Label('Yes') >> trigger_update_user
        if_active_present_and_1_64 >> rail.Label('No') >> catch_and_log_error

        if_userstatus_is_true >> rail.Label('Yes') >> if_active_present_and_0_67
        if_userstatus_is_true >> rail.Label('No') >> catch_and_log_error

        if_active_present_and_0_67 >> rail.Label('Yes') >> trigger_disbale_user
        if_active_present_and_0_67 >> rail.Label('No') >> if_active_present_and_1_69

        if_active_present_and_1_69 >> rail.Label('Yes') >> trigger_update_user
        if_active_present_and_1_69 >> rail.Label('No') >> if_active_notpresent_or_blank_71

        trigger_disbale_user >> wait_for_process_disable_user >> catch_and_log_error

        trigger_update_user >> wait_for_process_update_user >> catch_and_log_error

        if_active_notpresent_or_blank_71 >> rail.Label('Yes') >> log_blank_userstatus_72 >> catch_and_log_error
        if_active_notpresent_or_blank_71 >> rail.Label('No') >> catch_and_log_error

        if_active_is_1_74 >> rail.Label('Yes') >> trigger_add_user >> wait_for_process_add_user
        if_active_is_1_74 >> rail.Label('No') >> if_active_0_or_blank_76

        if_active_0_or_blank_76 >> rail.Label('Yes') >> log_user_disabled_in_workday_77 >> catch_and_log_error
        if_active_0_or_blank_76 >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
