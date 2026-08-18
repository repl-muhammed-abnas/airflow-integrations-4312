
from datetime import timedelta
from airflow.models import Variable
import rail
from ingenta.custom_report_extract.utils.python_callable import build_user_split_list as _build_user_split_list

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ingenta_custom_report_extract_child_{config.instance}',
        description=f'Ingenta_custom_report_extract_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='if_request_jobid_present_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_jobid_present_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_jobid_present_2 = rail.IfOperator(
            task_id='if_request_jobid_present_2',
            test='''{{ dag_run.conf.jobid | is_truthy }}''',
            yes_task="load_csv_create_list_from_csv_3",
            no_task="log_to_sumo",
        )

        load_csv_create_list_from_csv_3 = rail.PythonOperator(
            task_id="load_csv_create_list_from_csv_3",
            python_callable=lambda dag_run: rail.load_all_records(
                dag_run.conf['rows'])
        )

        create_collection_create_list_from_csv_3 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_3',
            source="{{ dag_run.conf.rows }}",
            name="Data_to_be_exported_all_months",
            columns={
                'username': 'username',
                'userdepartmentname': 'userdepartmentname',
                'userdeptfortrans': 'userdeptfortrans',
                'projectdeptfortrans': 'projectdeptfortrans',
                'clientname': 'clientname',
                'projectname': 'projectname',
                'timeofftype': 'timeofftype',
                'month': 'month',
                'timeoffdays': 'timeoffdays',
                'netcontractdays': 'netcontractdays',
                'actualdays': 'actualdays',
                'allocateddays': 'allocateddays',
                'availabledays': 'availabledays',
                'actualvsplanned': 'actualvsplanned',
                'useruri': 'useruri'
            }
        )

        # Filter by month to process only this month's data
        filter_by_month = rail.QueryCollectionOperator(
            task_id='filter_by_month',
            query="""SELECT * FROM Data_to_be_exported_all_months WHERE Data_to_be_exported_all_months.month='{{ dag_run.conf.month }}'""",
            name="Data_to_be_exported1"
        )

        build_user_split_list = rail.PythonOperator(
            task_id='build_user_split_list',
            python_callable=lambda: _build_user_split_list(),
        )

        ingenta_report_data_add_entry_23 = rail.WriteLogOperator(
            task_id='ingenta_report_data_add_entry_23',
            log="{{ dag_run.conf.lookuptable }}",
            items=lambda: rail.result('build_user_split_list'),
            message="na",
            severity="",
            properties=lambda item: {
                "username": item['username'],
                "userdepartment_name|userdepttrans": f"{item['userdepartmentname']}|{item['userdeptfortrans']}",
                "projectdept_for_trans": item['projectdeptfortrans'],
                "clientname|projectname": f"{item['clientname']}|{item['projectname']}",
                "timeofftype": item['timeofftype'],
                "month|time_off_days": f"{item['month']}|{item['timeoffdays']}",
                "contractdays|actualdays": f"{item['netcontractdays']}|{item['actualdays']}",
                "allocateddays|availbledays": f"{item['allocateddays']}|{item['availabledays']}",
                "actual_vs_planned": item['actualvsplanned'],
                "jobid": rail.render_template("{{dag_run.conf.jobid}}")
            }
        )

        ingenta_report_data_add_entry_26 = rail.WriteLogOperator(
            task_id='ingenta_report_data_add_entry_26',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="",
            properties={
                "username": "{{ dag_run.conf.month }}" +" "+ "Summary",
                "userdepartment_name|userdepttrans": "|",
                "projectdept_for_trans": "",
                "clientname|projectname": "|",
                "timeofftype": "",
                "month|time_off_days": "{{ dag_run.conf.month }}|{{ dag_run.conf.nettimeoffdays }}",
                "contractdays|actualdays": "{{ dag_run.conf.netcontractdays }}|{{ dag_run.conf.actualdays }}",
                "allocateddays|availbledays": "{{ dag_run.conf.allocateddays }}|{{ dag_run.conf.availabledays }}",
                "actual_vs_planned": "{{ dag_run.conf.actualvsplanned }}",
                "jobid": "{{dag_run.conf.jobid}}"
            }
        )

        if_islast_downcase_equals_to_true_27 = rail.IfOperator(
            task_id='if_islast_downcase_equals_to_true_27',
            test=lambda dag_run: dag_run.conf['islast'] == 'true',
            yes_task="process_mail_child",
            no_task="log_to_sumo",
        )

        process_mail_child = rail.TriggerDagRunOperator(
            task_id='process_mail_child',
            retries=0,
            trigger_dag_id=f'ingenta_custom_report_extract_send_file_child{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "jobid": "{{ dag_run.conf.jobid }}",
                "email": "{{ dag_run.conf.email }}",
                "lookuptable": "{{dag_run.conf.lookuptable}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_request_jobid_present_2
        if_request_jobid_present_2 >> rail.Label(
            'No') >> log_to_sumo
        if_request_jobid_present_2 >> rail.Label(
            'Yes') >> load_csv_create_list_from_csv_3 >> create_collection_create_list_from_csv_3
        create_collection_create_list_from_csv_3 >> filter_by_month >> build_user_split_list
        build_user_split_list >> ingenta_report_data_add_entry_23 >> ingenta_report_data_add_entry_26 >> if_islast_downcase_equals_to_true_27
        if_islast_downcase_equals_to_true_27 >> rail.Label(
            'Yes') >> process_mail_child >> log_to_sumo

        if_islast_downcase_equals_to_true_27 >> rail.Label('No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
