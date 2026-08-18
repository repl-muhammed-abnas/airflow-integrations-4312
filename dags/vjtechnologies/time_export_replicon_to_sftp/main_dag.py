from datetime import timedelta
from airflow.models import Variable
from vjtechnologies.time_export_replicon_to_sftp.utils import python_callable
import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vjtechnologies_time_export_master_{config.instance}',
        description=f'vjtechnologies_time_export_master_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                 config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_scripts'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_scripts',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id="get_all_scripts",
            endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'TimeExport', 'uri', '')
        )

        get_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            # pylint: disable=unnecessary-lambda
            data_handler=lambda response: python_callable.filter_company_slug_list(config, response)
        )

        process_each_enabled_divisions = rail.TriggerDagRunForEachItemOperator(
            task_id = "process_each_enabled_divisions",
            items= "{{ result('get_enabled_divisions')}}",
            trigger_dag_id= f'vjtechnologies_companycode_wise_timedata_export_child_{config.instance}',
            conf= {
                "companycode" : "{{item.displayText}}",
                "companycode_uri" : "{{item.uri}}",
                "file_format_uri": "{{ result('get_all_scripts') }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo

        can_run_batch_task >> rail.Label('No') >> get_all_scripts

        get_all_scripts >> get_enabled_divisions >> process_each_enabled_divisions >> \
            log_to_sumo

        return dag

rail.for_each_instance(create_dag)
