# pylint: disable=ungrouped-imports
from datetime import timedelta, datetime, timezone
import rail
from rws.custom_email_notification_v1.utils import python_callable_method
from airflow.models import Variable
from rws.custom_email_notification_v1.mappers import timezone_mapper
null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_1_dag_id,
        description=f'RWS send individual custom email notification for timesheets waiting for approval child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=Variable.get(
                    config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_timesheets_waiting_on_approver'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_timesheets_waiting_on_approver',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_timesheets_waiting_on_approver=rail.QueryCollectionOperator(
            task_id='query_timesheets_waiting_on_approver',
            query='''SELECT * FROM  formattedtimesheetdata WHERE formattedtimesheetdata.approveruri = "{{ dag_run.conf.approveruri }}"''',
        )

        if_for_approver_uri_present=rail.IfOperator(
            task_id='if_for_approver_uri_present',
            test=lambda dag_run: bool( dag_run.conf['approveruri'] and dag_run.conf['approveruri'] != "None"),
            yes_task="moravia_email_logs_search_entries",
            no_task="finish",
        )

        moravia_email_logs_search_entries=rail.FilterLogEntriesOperator(
            task_id='moravia_email_logs_search_entries',
            log= "{{ dag_run.conf.lookuptable }}",
            properties={
                'approvername': '{{dag_run.conf.approver}}',
                'approverid': '{{dag_run.conf.approveruri.split(":")[-1]}}',
                'date': '{{ current_time("%d/%m/%Y")}}'
            }
        )

        is_entry_present=rail.IfOperator(
            task_id='is_entry_present',
            test="{{ result('moravia_email_logs_search_entries', 'length') > 0 }}",
            yes_task="accumulate_item_as_skipped",
            no_task="is_entry_not_present",
        )

        accumulate_item_as_skipped=rail.SetVariableOperator(
            task_id='accumulate_item_as_skipped',
            name='Skipped Emails',
            append=True,
            value=python_callable_method.get_items_to_add,
        )

        is_entry_not_present=rail.IfOperator(
            task_id='is_entry_not_present',
            test="{{ result('moravia_email_logs_search_entries', 'length') == 0 }}",
            yes_task="log_timezone_name",
            no_task="finish",
        )

        log_timezone_name= rail.PythonOperator(
            task_id = 'log_timezone_name',
            python_callable= python_callable_method.get_iananame
        )

        log_timezone_difference=rail.PythonOperator(
            task_id='log_timezone_difference',
            python_callable= python_callable_method.get_timezone_hours_difference,
            op_args=[timezone_mapper.timezones,'{{result("log_timezone_name")}}']
        )

        def get_datetime_object_from_string(hourstring):
            return {
                "hours": datetime.strptime(hourstring,"%H.%M").hour,
                "minutes": datetime.strptime(hourstring,"%H.%M").minute
            } if len(hourstring.split('.')) > 1 else {
                "hours": datetime.strptime(hourstring,"%H").hour,
                "minutes": 0
            }

        get_time_object=rail.PythonOperator(
            task_id='get_time_object',
            python_callable= lambda:  get_datetime_object_from_string(rail.result('log_timezone_difference').split(' ')[-1])
        )

        is_it_ahead_in_time=rail.IfOperator(
            task_id='is_it_ahead_in_time',
            test="{{ result('log_timezone_difference') | matches('\\+')}}",
            yes_task="update_timezone_variable",
            no_task="is_it_behind_in_time",
        )

        update_timezone_variable=rail.SetVariableOperator(
            task_id='update_timezone_variable',
            append=False,
            name='{{ dag_run.conf.timezonevariablename }}',
            value= lambda: (datetime.now(timezone.utc) +
                                timedelta(hours=rail.result('get_time_object')["hours"],minutes=rail.result('get_time_object')["minutes"])).strftime('%H.%M')
        )

        is_it_behind_in_time=rail.IfOperator(
            task_id='is_it_behind_in_time',
            test="{{ result('log_timezone_difference') | matches('\\-')}}",
            yes_task="update_variable_timezone",
            no_task="get_hours_and_minutes",
        )

        update_variable_timezone=rail.SetVariableOperator(
            task_id='update_variable_timezone',
            append=False,
            name='{{ dag_run.conf.timezonevariablename }}',
            value= lambda: (datetime.now(timezone.utc) -
                                timedelta(hours=rail.result('get_time_object')["hours"],minutes=rail.result('get_time_object')["minutes"])).strftime('%H.%M')
        )

        get_variable_value = rail.GetVariableOperator(
            task_id = 'get_variable_value',
            name= '{{ dag_run.conf.timezonevariablename }}'
        )

        get_hours_and_minutes=rail.PythonOperator(
            task_id='get_hours_and_minutes',
            python_callable= lambda: float(rail.result('get_variable_value')['value'])
        )

        is_time_between_required_range=rail.IfOperator(
            task_id='is_time_between_required_range',
            test="{{ result('get_hours_and_minutes') >= 12.50 and result('get_hours_and_minutes') <= 13.15 }}",
            yes_task="get_timesheets_waiting_on_approver",
            no_task="is_time_outside_required_range",
        )


        get_timesheets_waiting_on_approver=rail.QueryCollectionOperator(
            task_id='get_timesheets_waiting_on_approver',
            query='''SELECT * FROM formattedtimesheetdata WHERE formattedtimesheetdata.approveruri = "{{ dag_run.conf.approveruri }}"''',
        )

        query_response_to_csv=rail.WriteCSVFileOperator(
            task_id='query_response_to_csv',
            source="{{ result('get_timesheets_waiting_on_approver') }}",
        )

        check_mail_status_for_approver=rail.PythonOperator(
            task_id='check_mail_status_for_approver',
            python_callable=python_callable_method.check_mail_status_for_approver,
        )

        is_mail_already_not_sent=rail.IfOperator(
            task_id='is_mail_already_not_sent',
            test='''{{ result('check_mail_status_for_approver') | is_falsy }}''',
            yes_task="trigger_child_dag_to_send_email",
            no_task="is_mail_already_sent",
        )

        trigger_child_dag_to_send_email=rail.TriggerDagRunOperator(
            task_id='trigger_child_dag_to_send_email',
            retries=0,
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(config.execution_timeout_days),
            conf={
                "approver": "{{ dag_run.conf.approver }}",
                "approveruri": "{{ dag_run.conf.approveruri }}",
                "inputdata": "{{ (result('query_response_to_csv'))}}",
                "parentjobid": "{{ dag_run_ecid() }}",
                "companykey": "{{ get_company_key()}}",
                "email_lookuptable": "{{ dag_run.conf.lookuptable}}",
                "date": "{{current_time('%d/%m/%Y')}}"
            }
        )

        accumulate_item_as_processed=rail.SetVariableOperator(
            task_id='accumulate_item_as_processed',
            name='Processed Emails',
            append=True,
            value=python_callable_method.get_items_to_add,
        )

        add_entry_moravia_email_logs_lookup_table=rail.WriteLogOperator(
            task_id="add_entry_moravia_email_logs_lookup_table",
            log="{{ dag_run.conf.lookuptable }}",
            message="Adding Entry",
            properties={
                'approvername': '{{dag_run.conf.approver}}',
                'approverid': '{{dag_run.conf.approveruri.split(":")[-1]}}',
                'status': 'Pending',
                'date': '{{current_time("%d/%m/%Y")}}'
            }
        )

        is_mail_already_sent=rail.IfOperator(
            task_id='is_mail_already_sent',
            test='''{{ result('check_mail_status_for_approver') | is_truthy }}''',
            yes_task="accumulateitem_as_skipped",
            no_task="is_time_outside_required_range",
        )

        accumulateitem_as_skipped=rail.SetVariableOperator(
            task_id='accumulateitem_as_skipped',
            name='Skipped Emails',
            append=True,
            value=python_callable_method.get_items_to_add,
        )

        is_time_outside_required_range=rail.IfOperator(
            task_id='is_time_outside_required_range',
            test="{{ result('get_hours_and_minutes') <= 12.50 or result('get_hours_and_minutes') >= 13.15 }}",
            yes_task="accumulate_itemas_skipped",
            no_task="finish",
        )

        accumulate_itemas_skipped=rail.SetVariableOperator(
            task_id='accumulate_itemas_skipped',
            name='Skipped Emails',
            append=True,
            value=python_callable_method.get_items_to_add,
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> query_timesheets_waiting_on_approver
        query_timesheets_waiting_on_approver >> if_for_approver_uri_present
        if_for_approver_uri_present >> rail.Label('Yes')  >> moravia_email_logs_search_entries >> is_entry_present
        is_entry_present >> rail.Label('Yes')  >> accumulate_item_as_skipped >> is_entry_not_present
        is_entry_present >> rail.Label('No') >> is_entry_not_present
        is_entry_not_present >> rail.Label('Yes')  >> log_timezone_name >> log_timezone_difference >> get_time_object >> is_it_ahead_in_time
        is_it_ahead_in_time >> rail.Label('Yes')  >> update_timezone_variable >> get_variable_value >> get_hours_and_minutes
        is_it_ahead_in_time >> rail.Label('No') >> is_it_behind_in_time
        is_it_behind_in_time >> rail.Label('Yes')  >> update_variable_timezone >> get_variable_value >> get_hours_and_minutes
        is_it_behind_in_time >> rail.Label('No') >> get_hours_and_minutes >> is_time_between_required_range
        is_time_between_required_range >> rail.Label(
            'Yes')  >> get_timesheets_waiting_on_approver >> query_response_to_csv >> check_mail_status_for_approver >> is_mail_already_not_sent
        is_mail_already_not_sent >> rail.Label(
            'Yes')  >> trigger_child_dag_to_send_email >> accumulate_item_as_processed
        accumulate_item_as_processed >> add_entry_moravia_email_logs_lookup_table >> is_time_outside_required_range
        is_mail_already_not_sent >> rail.Label('No') >> is_mail_already_sent
        is_mail_already_sent >> rail.Label('Yes')  >> accumulateitem_as_skipped >> is_time_outside_required_range
        is_mail_already_sent >> rail.Label('No') >> is_time_outside_required_range
        is_time_between_required_range >> rail.Label('No') >> is_time_outside_required_range
        is_time_outside_required_range >> rail.Label('Yes')  >> accumulate_itemas_skipped >> finish
        is_time_outside_required_range >> rail.Label('No') >> finish
        is_entry_not_present >> rail.Label('No') >> finish
        if_for_approver_uri_present >> rail.Label('No') >> finish

    return dag
rail.for_each_instance(create_dag)
