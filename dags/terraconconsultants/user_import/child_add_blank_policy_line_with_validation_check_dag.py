from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.request_payload import get_put_timeoff_with_initialblank


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_timeoffpolicy_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_addblankpolicyline_with_validation_check_{config.instance}',
        description=f'TerraconConsultants Child - Add blank policy line with validation check {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existingpolicy_schedule_for_timeoff'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existingpolicy_schedule_for_timeoff',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint='/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', dag_run.conf['timeoffuri'], 'policySetSchedule', '')
        )

        get_preventbalanceoverdraw_script = rail.RepliconServiceOperator(
            task_id='get_preventbalanceoverdraw_script',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        is_first_description_present = rail.IfOperator(
            task_id='is_first_description_present',
            test="{{ result('get_existingpolicy_schedule_for_timeoff') | first_or_default | \
                attr_or_default('description') | is_truthy }}",
            yes_task="past_policyset_schedule",
            no_task="dagrun_log_to_sumo",
        )

        def construct_policyschedule():
            policy_set_schedule = rail.result(
                'get_existingpolicy_schedule_for_timeoff')
            policy_schedule_entries = []
            count_list = []
            if policy_set_schedule:
                for item1 in policy_set_schedule:
                    if item1:
                        effective_datetime = datetime.strptime(
                            f"{item1['effectiveDate']['day']}/{item1['effectiveDate']['month']}/{item1['effectiveDate']['year']}",
                            '%d/%m/%Y') if item1.get('effectiveDate') else ''
                        if effective_datetime and effective_datetime.date() < datetime.now().date():
                            if item1['policySet']['timeOffValidationScripts']:
                                count_list.append({
                                    'count': item1['description'],
                                    'validationuri': rail.find_first_by_attr_and_get_attr(
                                        item1['policySet']['timeOffValidationScripts'], 'script.name',
                                        'Prevent balance overdraw', 'uri', ''),
                                    'overdrawvalue': rail.find_first_by_attr_and_get_attr(
                                        rail.find_first_by_attr_and_get_attr(
                                            item1['policySet']['timeOffValidationScripts'], 'script.name',
                                            'Prevent balance overdraw', 'additionalParameters', []), 'keyUri',
                                        'urn:replicon:script-key:parameter:maximum-overdraw', 'value.number', '')
                                })
                        parsed_item1 = json.loads(json.dumps(
                            item1, ensure_ascii=False).replace('"null"', '"effectiveDate"').replace(
                            '"script"', '"scriptTarget"'))
                        policy_schedule_entries.append(parsed_item1)
            validation_uris = [x['validationuri']
                               for x in count_list if x.get('validationuri')]
            overdraw_values = [x['overdraw_value']
                               for x in count_list if x.get('overdraw_value')]
            return {
                'policy_schedule_entries': policy_schedule_entries,
                'prevent_overdrawvalue_flag': validation_uris[-1] if validation_uris else '',
                'prevent_overdrawvalue': overdraw_values[-1] if overdraw_values else ''
            }
        past_policyset_schedule = rail.PythonOperator(
            task_id='past_policyset_schedule',
            python_callable=construct_policyschedule
        )

        is_pastpolicyset_schedule = rail.IfOperator(
            task_id='is_pastpolicyset_schedule',
            test="{{ result('past_policyset_schedule') | is_truthy }}",
            yes_task="put_timeoffpolicy_with_initialbalance_as_blank",
            no_task="dagrun_log_to_sumo"
        )

        put_timeoffpolicy_with_initialbalance_as_blank = rail.RepliconServiceOperator(
            task_id='put_timeoffpolicy_with_initialbalance_as_blank',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_put_timeoff_with_initialblank
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_existingpolicy_schedule_for_timeoff >> get_preventbalanceoverdraw_script >> \
            is_first_description_present
        is_first_description_present >> rail.Label(
            'Yes') >> past_policyset_schedule >> is_pastpolicyset_schedule
        is_pastpolicyset_schedule >> rail.Label(
            'Yes') >> put_timeoffpolicy_with_initialbalance_as_blank >> dagrun_log_to_sumo
        is_pastpolicyset_schedule >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_first_description_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_timeoffpolicy_dag)
