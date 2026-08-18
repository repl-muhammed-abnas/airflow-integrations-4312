from datetime import datetime, timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_task_child_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_Compass_Labour_Type_and_Task_Automation- Process task child {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_run_child_process,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        has_startdate_enddate = rail.IfOperator(
            task_id='has_startdate_enddate',
            test="{{ dag_run.conf.startdate | is_truthy and dag_run.conf.enddate | is_truthy and dag_run.conf.name | is_truthy }}",
            yes_task="has_task_in_project",
            no_task="log_invalid_task",
        )

        has_task_in_project = rail.IfOperator(
            task_id='has_task_in_project',
            test="{{ dag_run.conf.name and dag_run.conf.project_tasks | find_first_by_attr_and_get_attr('task.name',dag_run.conf.name,'task.uri') | is_truthy }}",
            yes_task="has_same_task_info",
            no_task="create_task",
        )

        def format_replicon_date(date):
            if not date:
                return null
            return datetime(year=date['year'], month=date['month'], day=date['day']).strftime('%Y%m%d')

        def do_has_same_task_info():
            conf = rail.get_current_context()['dag_run'].conf
            task_info = rail.find_first_by_attr_and_get_attr(
                conf['project_tasks'], 'task.name', conf['name'])['task']
            return conf['code'] == task_info['code'] and \
                conf['startdate'] == format_replicon_date(task_info['timeEntryDateRange']['startDate']) and \
                conf['enddate'] == format_replicon_date(
                    task_info['timeEntryDateRange']['endDate'])

        has_same_task_info = rail.IfOperator(
            task_id='has_same_task_info',
            test=do_has_same_task_info,
            yes_task="log_same_record",
            no_task="update_task",
        )

        log_same_record = rail.WriteLogOperator(
            task_id='log_same_record',
            log="{{ dag_run.conf.log }}",
            message='No change to task record',
            severity='Info',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '{{dag_run.conf.name}}',
                'billingrate': '',
                'message': 'No change to task record',
                'status': 'Skipped',
            }

        )

        update_task = rail.TriggerDagRunForEachItemOperator(
            task_id='update_task',
            retries=0,
            items=lambda: [rail.get_current_context()['dag_run'].conf],
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_update_task_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "log": item['log'],
                "name": item['name'],
                "description":  item['code'],
                "enddate": item['enddate'],
                "startdate": item['startdate'],
                "projecturi": item['project_info']['project']['uri'],
                "projectname": item['wbs'],
                "taskuri": rail.find_first_by_attr_and_get_attr(item['project_tasks'], 'task.name', item['name'], 'task.uri'),
                "resourceandtaskassignment": item['billingrates'],
            }
        )

        wait_for_update_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_task',
            dag_runs='{{ result("update_task") }}',
            execution_timeout=timedelta(days=14),
        )

        create_task = rail.TriggerDagRunForEachItemOperator(
            task_id='create_task',
            retries=0,
            items=lambda: [rail.get_current_context()['dag_run'].conf],
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_create_task_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "log": item['log'],
                "name": item['name'],
                "description":  item['code'],
                "enddate": item['enddate'],
                "startdate": item['startdate'],
                "projecturi": item['project_info']['project']['uri'],
                "projectname": item['wbs'],
                "resourceandtaskassignment": item['billingrates'],
            }
        )

        wait_for_create_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_task',
            dag_runs='{{ result("create_task") }}',
            execution_timeout=timedelta(days=14),
        )

        log_invalid_task = rail.WriteLogOperator(
            task_id='log_invalid_task',
            log="{{ dag_run.conf.log }}",
            message='Start date / End date / Task is not available',
            severity='Exception',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '{{dag_run.conf.name}}',
                'billingrate': '',
                'message': 'Start date / End date is not available',
                'status': 'Exception',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message()}}',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '{{dag_run.conf.name}}',
                'billingrate': '',
                'message': '{{ get_error_message()}}',
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        has_startdate_enddate >> rail.Label('Yes') >> has_task_in_project
        has_startdate_enddate >> rail.Label(
            'No') >> log_invalid_task >> log_to_sumo
        has_task_in_project >> rail.Label('Yes') >> has_same_task_info
        has_task_in_project >> rail.Label(
            'No') >> create_task >> wait_for_create_task >> catch_and_log_errors >> log_to_sumo
        has_same_task_info >> rail.Label(
            'Yes') >> log_same_record >> catch_and_log_errors >> log_to_sumo
        has_same_task_info >> rail.Label(
            'no') >> update_task >> wait_for_update_task >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
