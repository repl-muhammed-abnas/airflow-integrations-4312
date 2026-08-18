from datetime import timedelta
from airflow.models import Variable

import rail
from kla.user_import_usa_v2.mapper.general_mapper import general_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_update_user_timeoff_v2_{config.instance}',
        description=f'KLATencor Update User - Time Off V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def get_conf():
            return rail.get_current_context()['dag_run'].conf

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_mapper_entries_timeofftype'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_mapper_entries_timeofftype',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_mapper_entries_timeofftype = rail.PythonOperator(
            task_id='search_mapper_entries_timeofftype',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "time off type"
                                                     and x["Employee type"] == get_conf()['employeetype'], general_mapper)), {}).get('Value')
        )

        getenabled_time_offtypes = rail.RepliconServiceOperator(
            task_id='getenabled_time_offtypes',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        has_timeofftype_map_entry = rail.IfOperator(
            task_id='has_timeofftype_map_entry',
            test="{{ result('search_mapper_entries_timeofftype') | is_truthy }}",
            yes_task="log_message_removedelimiterfromthe_time_offtypeslisted",
        )

        log_message_removedelimiterfromthe_time_offtypeslisted = rail.PythonOperator(
            task_id='log_message_removedelimiterfromthe_time_offtypeslisted',
            python_callable=lambda: rail.result(
                'search_mapper_entries_timeofftype').split("|")
        )

        get_timeoff_uris = rail.PythonOperator(
            task_id='get_timeoff_uris',
            python_callable=lambda: list(map(lambda x:
                                             rail.find_first_by_attr_and_get_attr(rail.result(
                                                 getenabled_time_offtypes.task_id), 'displayText', x, 'uri'),
                                             rail.result('log_message_removedelimiterfromthe_time_offtypeslisted')))
        )

        assignrequired_timeofftypes = rail.RepliconServiceOperator(
            task_id='assignrequired_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "timeOffTypeUris": rail.result('get_timeoff_uris')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> search_mapper_entries_timeofftype
        search_mapper_entries_timeofftype >> getenabled_time_offtypes >> has_timeofftype_map_entry
        has_timeofftype_map_entry >> rail.Label(
            'Yes') >> log_message_removedelimiterfromthe_time_offtypeslisted >> get_timeoff_uris >> assignrequired_timeofftypes >> finish
        has_timeofftype_map_entry >> rail.Label(
            'No') >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
