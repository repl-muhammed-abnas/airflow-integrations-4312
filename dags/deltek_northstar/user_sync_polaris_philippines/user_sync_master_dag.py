import json
import itertools
from datetime import timedelta
from airflow.models import Variable
import rail
from deltek_northstar.user_sync_polaris_philippines.utils import request_payload, python_callable
from deltek_northstar.user_sync_polaris_philippines.tasks.get_user_prereqs import get_user_prereqs_task_group
null = None

mandatory_fields = [
    'FIRST_NAME','LAST_NAME','EMPL_ID','EMAIL_ID','PAY_PERIOD'
]

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'deltek_costpoint_user_sync_{config.instance}',
        schedule_interval=config.master_dag_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=lambda: python_callable.do_get_last_run_date(config)
        )

        costpoint_user_sync_logs = rail.CreateLogOperator(
            task_id="costpoint_user_sync_logs"
        )

        can_use_conf_payload = rail.IfOperator(
            task_id='can_use_conf_payload',
            test=lambda: Variable.get(
                config.can_use_conf_payload_var_name, default_var='false').lower() == 'true',
            yes_task='get_conf_payload',
            no_task='get_users_from_costpoint'
        )

        get_conf_payload = rail.PythonOperator(
            task_id='get_conf_payload',
            python_callable=lambda: json.dumps(rail.get_dag_run_conf())
        )

        get_users_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_users_from_costpoint',
            endpoint=config.api_endpoint,
            company=config.deltek_cospoint_company_ids,
            data=lambda: request_payload.get_costpoint_payload(rail.result('get_last_run_date'))
        )

        if_costpoint_user_present = rail.IfOperator(
            task_id='if_costpoint_user_present',
            test=python_callable.is_costpoint_user_present,
            yes_task="supervisor_processing_log",
            no_task="delete_this_dagrun",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        supervisor_processing_log = rail.CreateLogOperator(
            task_id='supervisor_processing_log',
        )

        get_polaris_users = rail.PythonOperator(
            task_id='get_polaris_users',
            python_callable=lambda: python_callable.get_polaris_users()
        )

        create_valid_data_collection = rail.CreateCollectionOperator(
            task_id='create_valid_data_collection',
            source="{{ result('get_polaris_users') | to_json }}",
            columns={
                'FIRST_NAME': 'first_name',
                'LAST_NAME': 'last_name',
                'LAST_FIRST_NAME': 'display_name',
                'EMPL_ID': 'empl_id',
                'EMAIL_ID': 'email_id',
                'ORIG_HIRE_DT': 'current_hire_date',
                'ADJ_HIRE_DT': 'past_hire_date',
                'TERM_DT': 'termination_date',
                'S_EMPL_STATUS_CD': 'status',
                'PERS_ACT_RSN_CD': 'personal_action_code',
                'BILL_LAB_CAT_CD': 'plc',
                'EFFECT_DT': 'effect_date',
                'DETL_JOB_CD': 'detail_job_title',
                'MGR_EMPL_ID': 'mgr_empl_id',
                'TAXBLE_ENTITY_ID': 'taxble_entity_id',
                'TAXBLE_ENTITY_NAME': 'taxble_entity_name',
                'ORG': 'org',
                'ORG_ID': 'org_id',
                'ORG_NAME': 'org_name',
                'S_EMPL_TYPE_CD': 'employee_type',
                'S_HRLY_SAL_CD': 'rate_type',
                'COUNTRY_CD': 'country',
                'HR_ORG_ID': 'hr_organization',
                'TS_PD_CD': 'timesheet_cycle',
                'TC_WORK_SCHED_CD': 'work_schedule',
                'GENL_LAB_CAT_CD': 'glc',
                'TRN_CRNCY_CD': 'home_currency',
                'POLARIS_ROLE': 'polaris_role',
                'TITLE_DESC': 'title_desc',
                'PAY_PERIOD': 'pay_period_code',
                'ROSTER': 'shift_schedule_name',
            },
            name="input_data"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data WHERE
                    NULLIF("first_name","") IS NULL OR
                    NULLIF("last_name", "") IS NULL OR
                    NULLIF("empl_id", "") IS NULL OR
                    NULLIF("email_id", "") IS NULL OR
                    NULLIF("pay_period_code", "") IS NULL
                """,
            name="invalid_records"
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log = "{{ result('costpoint_user_sync_logs') }}",
            items='{{result("query_invalid_records")}}',
            message=python_callable.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'lastname': item['last_name'],
                'firstname': item['first_name'],
                'loginname':  item['email_id'],
                'employeeid': item['empl_id'],
                'useruri': '',
                'manager': item['mgr_empl_id'],
                'action': 'Validation',
                'status': 'Exception',
                'details': python_callable.get_mandatory_fields_exception_message(item),
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM input_data WHERE
                    NULLIF("first_name","") IS NOT NULL AND
                    NULLIF("last_name", "") IS NOT NULL AND
                    NULLIF("empl_id", "") IS NOT NULL AND
                    NULLIF("email_id", "") IS NOT NULL AND
                    NULLIF("pay_period_code", "") IS NOT NULL
                """,
            name="valid_records"
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task="process_groups",
            no_task="process_log_generation"
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.process_groups,
            conf={
                'user_log': "{{ result('costpoint_user_sync_logs') }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_process_groups",
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        process_dropdowns = rail.TriggerDagRunOperator(
            task_id="process_dropdowns",
            trigger_dag_id=config.process_dropdowns,
            conf={
                'user_log': "{{ result('costpoint_user_sync_logs') }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_dropdowns = rail.WaitForDagRunsSensor(
            task_id="wait_process_dropdowns",
            dag_runs="{{ result('process_dropdowns') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        dummy_get_user_prereqs, get_user_prereqs= get_user_prereqs_task_group()

        dummy_process_users = rail.EmptyOperator(
            task_id='dummy_process_users'
        )

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_process_users,
            trigger_dag_id=config.process_users,
            conf=lambda item: request_payload.get_process_users_conf(config, item),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_users_{x+1}'), range(config.trigger_parallel_process_users))))),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs="{{ result('get_process_users_dag_ids') }}",
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        get_supervisorcheck_queued_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_queued_logs',
            log="{{ result('supervisor_processing_log') }}",
            severity='Pending',
            remove_filtered_entries=True
        )

        is_supervisorcheck_queued_logs = rail.IfOperator(
            task_id='is_supervisorcheck_queued_logs',
            test="{{ result('get_supervisorcheck_queued_logs', 'length') > 0 }}",
            yes_task='process_supervisor_child_dag',
            no_task='process_log_generation'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items=lambda: rail.load_all_records(rail.result('get_supervisorcheck_queued_logs')),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.processs_supervisor,
            conf=lambda item: {
                **dict(item['properties'].items()),
                'user_log': rail.result('costpoint_user_sync_logs'),
                'supervisor_permission_uri': request_payload.get_supervisor_permission_uri()
            }
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'userlogs': rail.result('gather_user_logs') if rail.result('gather_user_logs') else [],
                'otherlogs': rail.result('costpoint_user_sync_logs') if rail.result('costpoint_user_sync_logs') else [],
                'log_filename': f"""northstar_hris_employee_sync_{rail.render_template("{{current_time_in_specified_tz(fmt='%Y-%m-%dT%H-%M-%S') | replace(':', '-')}}")}_log.csv"""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_last_run_date >> costpoint_user_sync_logs >> can_use_conf_payload
        can_use_conf_payload >> rail.Label(
            'Yes') >> get_conf_payload >> if_costpoint_user_present
        can_use_conf_payload >> rail.Label('No') >> get_users_from_costpoint >> \
            if_costpoint_user_present
        if_costpoint_user_present >> rail.Label(
            'No') >> delete_this_dagrun 
        if_costpoint_user_present >> rail.Label('Yes') >> supervisor_processing_log >> \
        get_polaris_users >> create_valid_data_collection >> query_invalid_records >> has_invalid_records
        has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> query_valid_records
        has_invalid_records >> rail.Label(
            'No') >> query_valid_records >> has_valid_records
        has_valid_records >> rail.Label(
            'Yes') >> process_groups >> wait_process_groups >> process_dropdowns >> wait_process_dropdowns >> dummy_get_user_prereqs
        has_valid_records >> rail.Label(
            'No') >> process_log_generation

        get_user_prereqs >> dummy_process_users >> process_users >> get_process_users_dag_ids >> gather_user_logs >> get_supervisorcheck_queued_logs
        get_supervisorcheck_queued_logs >> is_supervisorcheck_queued_logs >> rail.Label('No') >> process_log_generation
        is_supervisorcheck_queued_logs >> rail.Label('Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> process_log_generation
        process_log_generation >> log_to_sumo

        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

        return dag

rail.for_each_instance(create_dag)
