from pendulum import datetime
import rail

from kmhavintegrationinc.custom_notification_for_task_milestone.utils import python_callable
from kmhavintegrationinc.custom_notification_for_task_milestone.tasks.milestone_task import process_task_milestone


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'kmhavintegrationinc_custom_notification_for_task_milestone_master_{config.instance}',
        description=f'Kmhavintegrationinc_custom_notification_for_task_milestone {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        get_milestone_details_report_uri = rail.RepliconReportDetailsOperator(
            task_id='get_milestone_details_report_uri',
            report_name=config.milestone_details_report_name,
        )

        get_task_milestone_details=rail.RepliconServiceOperator(
            task_id='get_task_milestone_details',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{result('get_milestone_details_report_uri').uri}}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
        )

        load_milestone_report_data=rail.LoadCSVFileOperator(
            task_id='load_milestone_report_data',
            document="{{ result('get_task_milestone_details').payload }}",
            headers=
            [
                'Project Name',
                'Task Name (Full Path)',
                'Task Status',
                'Task Estimated Hours',
                'Actual Hours',
                'Milestone',
                'Client Name',
                'Project Manager',
                'Project Manager Email'],
        )

        get_adminmail_report_uri = rail.RepliconReportDetailsOperator(
            task_id='get_adminmail_report_uri',
            report_name=config.admin_details_report_name,
        )

        get_adminmail_report_details=rail.RepliconServiceOperator(
            task_id='get_adminmail_report_details',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{result('get_adminmail_report_uri').uri}}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
        )

        load_adminmail_report_data = rail.LoadCSVFileOperator(
            task_id='load_adminmail_report_data',
            document="{{ result('get_adminmail_report_details').payload}}",
            headers=['User Name', 'User Email', 'Permission Name'],

        )

        get_adminmail_ids=rail.PythonOperator(
            task_id='get_adminmail_ids',
            python_callable=python_callable.get_adminmail_ids
        )

        task_milestone_logger = rail.CreateLogOperator(
            task_id="task_milestone_logger",
            tenant_wide_name="task_milestone_log_table",
            existing_log_mode="append",
        )

        foreach_item_in_task_milestone_do= rail.ForEachOperator(
            task_id='foreach_item_in_task_milestone_do',
            items="{{ result('load_milestone_report_data')}}",
            start_task='is_project_name_present',
            end_task = 'finish'
        )

        is_project_name_present = rail.IfOperator(
            task_id='is_project_name_present',
            test="{{result('foreach_item_in_task_milestone_do')['Project Name'] | is_truthy }}",
            yes_task="get_task_milestone_value",
            no_task="finish",
        )

        get_task_milestone_value=rail.PythonOperator(
            task_id='get_task_milestone_value',
            python_callable= python_callable.get_milestone
        )

        is_milestone_value_range_30_60 = rail.IfOperator(
            task_id='is_milestone_value_range_30_60',
            test="{{result('get_task_milestone_value') > 29.99 and result('get_task_milestone_value') < 60.00 }}",
            yes_task="search_task_milestone_entry_30-60",
            no_task="is_milestone_value_range_60_90",
        )

        task_milestone_30_entry, task_milestone_30_exit = process_task_milestone("{{result('foreach_item_in_task_milestone_do').get('Task Name (Full Path)')}}",
            "{{result('foreach_item_in_task_milestone_do').get('Project Name')}}","30-60","30%",config.tenant_email)

        is_milestone_value_range_60_90 = rail.IfOperator(
            task_id='is_milestone_value_range_60_90',
            test="{{ result('get_task_milestone_value') > 59.99  and result('get_task_milestone_value') < 89.99 }}",
            yes_task="search_task_milestone_entry_60-90",
            no_task="is_milestone_value_range_90_100",
        )

        task_milestone_60_entry, task_milestone_60_exit= process_task_milestone("{{result('foreach_item_in_task_milestone_do').get('Task Name (Full Path)')}}",
            "{{result('foreach_item_in_task_milestone_do').get('Project Name')}}","60-90","60%",config.tenant_email)

        is_milestone_value_range_90_100 = rail.IfOperator(
            task_id='is_milestone_value_range_90_100',
            test="{{ result('get_task_milestone_value') > 90.00  and result('get_task_milestone_value') < 99.99 }}",
            yes_task="search_task_milestone_entry_90-100",
            no_task="is_milestone_value_range_above_100",
        )

        task_milestone_90_entry, task_milestone_90_exit= process_task_milestone("{{result('foreach_item_in_task_milestone_do').get('Task Name (Full Path)')}}",
            "{{result('foreach_item_in_task_milestone_do').get('Project Name')}}","90-100","90%",config.tenant_email)

        is_milestone_value_range_above_100= rail.IfOperator(
            task_id='is_milestone_value_range_above_100',
            test="{{ result('get_task_milestone_value') > 99.99 }}",
            yes_task="search_task_milestone_entry_100",
            no_task="finish",
        )

        task_milestone_100_entry, task_milestone_100_exit = process_task_milestone(
            "{{result('foreach_item_in_task_milestone_do').get('Task Name (Full Path)')}}",
            "{{result('foreach_item_in_task_milestone_do').get('Project Name')}}","100","100%",config.tenant_email)

        finish = rail.EmptyOperator(
            task_id='finish',
        )


        get_milestone_details_report_uri >> get_task_milestone_details >> load_milestone_report_data

        load_milestone_report_data >> get_adminmail_report_uri >> get_adminmail_report_details >> load_adminmail_report_data

        load_adminmail_report_data >> get_adminmail_ids >> task_milestone_logger >> foreach_item_in_task_milestone_do >> is_project_name_present

        is_project_name_present >> rail.Label(
            'Yes') >> get_task_milestone_value >> is_milestone_value_range_30_60

        is_project_name_present >> rail.Label(
            'No') >> finish

        is_milestone_value_range_30_60 >> rail.Label('Yes') >> task_milestone_30_entry
        is_milestone_value_range_30_60 >> rail.Label('No') >> is_milestone_value_range_60_90

        is_milestone_value_range_60_90 >> rail.Label('Yes') >> task_milestone_60_entry
        is_milestone_value_range_60_90 >> rail.Label('No') >> is_milestone_value_range_90_100

        is_milestone_value_range_90_100 >> rail.Label('Yes') >> task_milestone_90_entry
        is_milestone_value_range_90_100 >> rail.Label('No') >> is_milestone_value_range_above_100

        is_milestone_value_range_above_100 >> rail.Label('Yes') >> task_milestone_100_entry
        is_milestone_value_range_above_100 >> rail.Label('No') >> finish

        task_milestone_30_exit >> finish
        task_milestone_60_exit >> finish
        task_milestone_90_exit >> finish
        task_milestone_100_exit >> finish

        foreach_item_in_task_milestone_do >> finish

    return dag

rail.for_each_instance(create_dag)
