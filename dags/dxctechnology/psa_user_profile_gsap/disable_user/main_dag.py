from datetime import timedelta
from pendulum import datetime
import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/psa_user_profile_gsap/disable_user/config.py


def create_disable_user_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_psa_userprofiles_disable_master_{config.instance}',
        description=f'DXC_PSA UserProfiles_Disable_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.master_dag_interval,
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_contractors_report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='contractor_report_generation',
            report_params={
                'reportParameters': [
                    {
                        'reportUri': "{{ result('get_report_details').uri }}",
                        'filterValues': [],
                        'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ result('contractor_report_generation.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_contractor_report_generation',
            no_task='report_has_data'
        )

        fail_contractor_report_generation = rail.FailOperator(
            task_id='fail_contractor_report_generation',
            message="{{ result('contractor_report_generation.get_report_result').reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('contractor_report_generation.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='No Data for Contractors'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('contractor_report_generation.get_report_result').reportGenerationResults[0].payload }}"
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id='report_data_collection',
            source="{{ result('load_report_data') }}",
            name='userdata',
            columns={
                'User Name': 'user',
                'User Status': 'status',
                'Employee Type (Current)': 'employeetype',
                'UserUri': 'uri',
                'User End Date': 'enddate',
                'DayDiff': 'daydiff'
            }
        )

        query_users_to_disable = rail.QueryCollectionOperator(
            task_id='query_users_to_disable',
            query="SELECT * FROM userdata WHERE enddate IS NOT NULL AND daydiff < 0 AND status = 'Enabled'"
        )

        users_to_disable = rail.IfOperator(
            task_id='users_to_disable',
            test="{{ result('query_users_to_disable', 'length') > 0 }}",
            yes_task='disable_user_child',
            no_task='finish'
        )

        disable_user_child = rail.TriggerDagRunForEachItemOperator(
            task_id='disable_user_child',
            retries=0,
            items="{{ result('query_users_to_disable') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_psa_userprofiles_disable_child_{config.instance}',
            conf=lambda item: {k.lower(): v for k, v in item.items() if k in (
                'user', 'uri', 'enddate')}
        )

        wait_for_disable_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_user_child',
            dag_runs='{{ result("disable_user_child") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_disable_user_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disable_user_errors',
            dag_runs="{{ result('disable_user_child') }}",
            dagrun_task_id='catch_disable_user_error',
            flatten=True
        )

        is_disable_user_error = rail.IfOperator(
            task_id='is_disable_user_error',
            test="{{ result('gather_disable_user_errors') | map_to_attr('useruri') | length > 0 }}",
            yes_task='fail_disable_user_error',
            no_task='finish'
        )

        fail_disable_user_error = rail.FailOperator(
            task_id='fail_disable_user_error',
            message='Errors noticed while disabling few users'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label(
            'Yes') >> fail_contractor_report_generation

        is_report_failed >> rail.Label(
            'No') >> report_has_data

        report_has_data >> rail.Label(
            'Yes') >> load_report_data >> report_data_collection >> query_users_to_disable >> users_to_disable

        users_to_disable >> rail.Label(
            'Yes') >> disable_user_child >> wait_for_disable_user_child >> gather_disable_user_errors >> is_disable_user_error

        is_disable_user_error >> rail.Label(
            'Yes') >> fail_disable_user_error

        is_disable_user_error >> rail.Label(
            'No') >> finish

        users_to_disable >> rail.Label(
            'No') >> finish

        report_has_data >> rail.Label(
            'No') >> fail_no_report_data

        return dag


rail.for_each_instance(create_disable_user_main_dag)
