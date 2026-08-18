from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/balparag3/project_import/config.py


def create_child_process_project_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'balparag3_projectimport_child_process_project_{config.instance}',
        description=f'Balparag3 Process Project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_project_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='log_dagrun_to_sumo'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        query_project_from_code_name = rail.QueryCollectionOperator(
            task_id='query_project_from_code_name',
            query="""SELECT * FROM validatedinputdata WHERE
                    projectcode = :projectcode
                    AND projectname = :projectname""",
            query_params={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'projectname': '{{ dag_run.conf.projectname }}'
            }
        )

        def load_project_record_from_collection():
            project_records = rail.load_all_records(
                rail.result('query_project_from_code_name'))
            project_record = project_records[0] if project_records else {}
            users = list(set(x['users']
                         for x in project_records if x['projectcode']))
            department = list(set(x['department']
                              for x in project_records if x['projectcode']))
            billingrates = list(set(x['billingrates']
                                for x in project_records if x['projectcode']))
            return {
                **{k: v for k, v in project_record.items() if k not in ('users',
                                                                        'department', 'billingrates')
                   },
                **{
                    'users': rail.smartjoin_by_delim(users, ';'),
                    'department': rail.smartjoin_by_delim(department, ';'),
                    'billingrates': rail.smartjoin_by_delim(billingrates, ';'),
                    'userbillingrates': list(map(lambda x: {
                        'user_name': x,
                        'billing_rates': list(set(y['billingrates'] for y in project_records if y['users'] == x))
                    }, users)),
                    'departmentbillingrates': list(map(lambda x: {
                        'department_name': x,
                        'billing_rates': list(set(y['billingrates'] for y in project_records if y['department'] == x))
                    }, department))
                }
            }
        load_project_record = rail.PythonOperator(
            task_id='load_project_record',
            python_callable=load_project_record_from_collection
        )

        is_user_department_not_present = rail.IfOperator(
            task_id='is_user_department_not_present',
            test="{{ result('load_project_record').users | is_falsy and \
                result('load_project_record').department | is_falsy }}",
            yes_task='write_project_exception',
            no_task='process_individual_project'
        )

        write_project_exception = rail.WriteLogOperator(
            task_id="write_project_exception",
            log="{{ result('create_log') }}",
            severity='Exception',
            message="Department and Users both field is blank",
            properties={
                'project_code': '{{ dag_run.conf.projectcode }}',
                'project_name': '{{ dag_run.conf.projectname }}',
                'status': 'Exception',
                'details': 'Department and Users both field is blank',
                'type': 'project'
            }
        )

        process_individual_project = rail.TriggerDagRunForEachItemOperator(
            task_id='process_individual_project',
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'balparag3_projectimport_child_{config.instance}',
            conf=lambda item: {
                **dict(rail.result('load_project_record')),
                **{k: v for k, v in item.items() if k not in ('get_enabled_locations', 'projectcode',
                                                              'projectname', '_ancestry', '_ecid',
                                                              '_replication_position')},
                **{
                    'location_uri': rail.find_first_by_attr_and_get_attr(
                        item['get_enabled_locations'], 'displayText', rail.result(
                            'load_project_record')['location'], 'uri', ''),
                    'log': rail.result('create_log')
                }
            }
        )

        wait_for_process_individual_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_individual_project',
            dag_runs='{{ result("process_individual_project") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> query_project_from_code_name >> load_project_record >> is_user_department_not_present

        is_user_department_not_present >> rail.Label(
            'Yes') >> write_project_exception >> log_dagrun_to_sumo

        is_user_department_not_present >> rail.Label(
            'No') >> process_individual_project >> wait_for_process_individual_project >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_process_project_dag)
