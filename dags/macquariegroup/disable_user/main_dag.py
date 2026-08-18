from datetime import timedelta
from pendulum import datetime
import rail
from airflow.models import Variable

def create_disable_user_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_userprofile_disable_master_{config.instance}",
        description=f'DXC_Fieldglass CWFUserProfiles_Disable_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.aus_timezone),
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
            report_name=config.user_disable_report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='report_generation',
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
            test="{{ result('report_generation.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ result('report_generation.get_report_result').reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('report_generation.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='No Data for In report'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('report_generation.get_report_result').reportGenerationResults[0].payload }}"
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
            query="SELECT * FROM userdata WHERE enddate IS NOT NULL AND daydiff < 1 AND status = 'Enabled'"
        )

        get_default_supervisor = rail.PythonOperator(
            task_id = "get_default_supervisor",
            python_callable=lambda: Variable.get(
                config.default_supervisor)
        )

        def get_default_supervisor_filter(response):
            def get_value(item, index, pluck_key):
                return item[index][pluck_key] if item[index]['dataType'] != "urn:replicon:list-type:null" else ""

            if not response['rows']:
                return {}
            res = list(filter(lambda x: x['login_name'] == rail.result("get_default_supervisor") and x['enabled'].lower() == "true",
                            map(lambda data: {
                                'name': get_value(data['cells'], 0, 'textValue'),
                                'uri': get_value(data['cells'], 0, 'uri'),
                                "enabled": get_value(data['cells'], 1, 'textValue'),
                                "login_name": get_value(data['cells'],2,'textValue')
                            }, response['rows'])))
            if not res:
                return {}
            return res[0]

        get_default_supervisor_from_replicon = rail.RepliconServiceOperator(
            task_id="get_default_supervisor_from_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:user-list-column:user-name",
                        "urn:replicon:user-list-column:enabled",
                        "urn:replicon:user-list-column:login-name"
                    ],
                "sort": [],
                "filterExpression": {
                        "leftExpression": {
                            "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                        },
                        "operatorUri": "urn:replicon:filter-operator:text-search",
                        "rightExpression": {
                            "value": {
                                "text": "{{ result('get_default_supervisor') }}"  # Hard Coded in airflow variable
                            }
                        }
                        }
            },
            data_handler=get_default_supervisor_filter
        )

        has_default_supervisor_found = rail.IfOperator(
            task_id = "has_default_supervisor_found",
            test= "{{ result('get_default_supervisor_from_replicon') | is_truthy}}",
            yes_task= "users_to_disable",
            no_task= "fail_default_supervisor_not_found"
        )

        fail_default_supervisor_not_found = rail.FailOperator(
            task_id = "fail_default_supervisor_not_found",
            message=f"failed as default supervisor {config.default_supervisor} not found in instance"
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
            trigger_dag_id=f"macquarie_userprofile_disable_child_{config.instance}",
            conf=lambda item: {
                **{k.lower(): v for k, v in item.items() if k in (
                'user', 'uri', 'enddate')},
                **{"default_supervisor_uri": rail.result("get_default_supervisor_from_replicon").get('uri', '')}
                }
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
            'Yes') >> fail_report_generation

        is_report_failed >> rail.Label(
            'No') >> report_has_data

        report_has_data >> rail.Label(
            'Yes') >> load_report_data >> report_data_collection >> query_users_to_disable >> get_default_supervisor\
                >> get_default_supervisor_from_replicon
        get_default_supervisor_from_replicon >> has_default_supervisor_found>> rail.Label("Yes") >> users_to_disable
        has_default_supervisor_found >> rail.Label("No") >> fail_default_supervisor_not_found

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
