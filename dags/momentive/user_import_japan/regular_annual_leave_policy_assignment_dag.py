from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable
from momentive.user_import_japan.mappers.momentive_annual_paid_leave_mapper import momentive_annual_paid_leave_regular_mapper


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_regular_assignment_dag_id,
        description=f'Momentive_user_sync_child_annual_leave_policy_regular_assignment_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='determine_employment_month_and_jan1st_check'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='determine_employment_month_and_jan1st_check',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # Parse hire date and determine employment month
        determine_employment_month_and_jan1st_check = rail.PythonOperator(
            task_id='determine_employment_month_and_jan1st_check',
            python_callable=lambda dag_run: {
                'day': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').day,
                'month': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').month,
                'year': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').year,
                'is_january_first': datetime.strptime(dag_run.conf['startdate'], '%Y-%m-%d').strftime('%d-%m') == '01-01'
            }
        )
        
        get_startdate_month = rail.PythonOperator(
            task_id='get_startdate_month',
            python_callable=lambda: python_callable.get_startdate_month(
                rail.result('determine_employment_month_and_jan1st_check').get('month'), rail.result('determine_employment_month_and_jan1st_check').get('is_january_first')
            )
        )

        search_timeofftypetobeused_acctostartdatemonth =rail.PythonOperator(
            task_id='search_timeofftypetobeused_acctostartdatemonth',
            python_callable=lambda dag_run:  list(filter(lambda x: x["startdate"] == rail.result("get_startdate_month"), momentive_annual_paid_leave_regular_mapper))
        )

        get_req_timeoff_type_uri = rail.RepliconServiceOperator(
            task_id='get_req_timeoff_type_uri',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('search_timeofftypetobeused_acctostartdatemonth')[0]['value'], 'uri')
        )

        if_req_timeoff_type_uri_present= rail.IfOperator(
            task_id='if_req_timeoff_type_uri_present',
            test="{{ result('get_req_timeoff_type_uri') | is_truthy }}",
            yes_task='get_default_policy_for_timeofftype',
            no_task='catch_error'
        )

        get_default_policy_for_timeofftype = rail.RepliconServiceOperator(
            task_id='get_default_policy_for_timeofftype',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda:{
                "timeOffTypeUri": rail.result('get_req_timeoff_type_uri')
            }
        )

        # Optimized:
        extract_policyset = rail.PythonOperator(
            task_id='extract_policyset',
            python_callable=lambda dag_run: python_callable.build_timeoff_policy_with_offset_check(
                rail.result('get_default_policy_for_timeofftype'),
                dag_run
            )
        )

        if_policy_entries_present = rail.IfOperator(
            task_id='if_policy_entries_present',
            test=lambda: bool(rail.result('extract_policyset')),
            yes_task='assign_annual_leave_policy_regular',
            no_task='catch_error'
        )

        assign_annual_leave_policy_regular = rail.RepliconServiceOperator(
            task_id='assign_annual_leave_policy_regular',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('extract_policyset')
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Regular Annual Leave Assignment for user ; {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_error') if rail.result('catch_error') else ""
        )
        
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> determine_employment_month_and_jan1st_check

        determine_employment_month_and_jan1st_check >> get_startdate_month >> search_timeofftypetobeused_acctostartdatemonth >> \
            get_req_timeoff_type_uri >> if_req_timeoff_type_uri_present >> rail.Label("Yes") >> get_default_policy_for_timeofftype
        
        get_default_policy_for_timeofftype >> extract_policyset >> if_policy_entries_present >> rail.Label("Yes") >> assign_annual_leave_policy_regular >> catch_error
        if_policy_entries_present >> rail.Label("No") >> catch_error

        if_req_timeoff_type_uri_present >> rail.Label("No") >> catch_error


        return dag 


rail.for_each_instance(create_dag)
