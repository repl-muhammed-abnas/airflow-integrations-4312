from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.user_sync_v1.utils import python_callable

def create_disable_user_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_status_main_dagid,
        description=config.update_user_status_main_dagid,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.disable_master_dag_interval,
        max_active_runs=config.disable_master_dag_active_runs,
    ) as dag:
        
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_report_details',
            end_task='update_user_status_finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_disable_report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='user_report_generation',
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
            test="{{ result('user_report_generation.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_user_report_generation',
            no_task='report_has_data'
        )

        fail_user_report_generation = rail.FailOperator(
            task_id='fail_user_report_generation',
            message="{{ result('user_report_generation.get_report_result').reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('user_report_generation.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='No Data for Contractors'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('user_report_generation.get_report_result').reportGenerationResults[0].payload }}"
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
                'DayDiff': 'daydiff',
                'Return Date from Leave': 'returndatefromleave'
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
            no_task='query_users_to_enable'
        )

        disable_user_child = rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_user_child',
            items="{{ result('query_users_to_disable') }}",
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': "{{ item.uri }}"
            }
        )

        query_users_to_enable = rail.QueryCollectionOperator(
            task_id='query_users_to_enable',
            query="SELECT * FROM userdata WHERE NULLIF(returndatefromleave, '') IS NOT NULL AND status = 'Disabled'"
        )

        get_uris_to_enable = rail.PythonOperator(
            task_id = "get_uris_to_enable",
            python_callable=python_callable.get_uri_to_enable
        )

        users_to_enable = rail.IfOperator(
            task_id='users_to_enable',
            test="{{ result('get_uris_to_enable') | length > 0 }}",
            yes_task='enable_user_child',
            no_task='update_user_status_finish'
        )

        enable_user_child = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_user_child',
            items="{{ result('get_uris_to_enable') }}",
            endpoint='services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': "{{ item.uri }}"
            }
        )

        update_user_status_finish = rail.EmptyOperator(
            task_id = "update_user_status_finish"
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> update_user_status_finish
        can_run_batch_task >> rail.Label('No') >> get_report_details

        get_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label('Yes') >> fail_user_report_generation
        is_report_failed >> rail.Label('No') >> report_has_data

        report_has_data >> rail.Label('Yes') >> load_report_data >> report_data_collection >> query_users_to_disable >> users_to_disable
        report_has_data >> rail.Label('No') >> fail_no_report_data

        users_to_disable >> rail.Label('Yes') >> disable_user_child >> query_users_to_enable
        users_to_disable >> rail.Label('No') >> query_users_to_enable

        query_users_to_enable >> get_uris_to_enable >> users_to_enable
        
        users_to_enable >> rail.Label('Yes') >> enable_user_child >> update_user_status_finish
        users_to_enable >> rail.Label('No') >> update_user_status_finish

        return dag

rail.for_each_instance(create_disable_user_main_dag)
