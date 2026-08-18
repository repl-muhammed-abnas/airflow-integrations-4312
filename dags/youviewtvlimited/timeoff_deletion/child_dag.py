
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'youviewtvlimited_timeoff_deletion_child_{config.instance}',
        description=f'Time off deletion_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_4 = rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='Action',
            value=None
        )

        declare_list_5 = rail.SetVariableOperator(
            task_id='declare_list_5',
            append=False,
            name='timeofflist',
            value=[]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_time_off_details_for_user_and_date_range2_7 = rail.RepliconServiceOperator(
            task_id='get_time_off_details_for_user_and_date_range2_7',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda: {
                "userUri": rail.get_dag_run_conf()['userURI'],
                "dateRange": {
                    "startDate": rail.parse_date(rail.get_dag_run_conf()['startdate'], '%d/%m/%Y'),
                    "endDate": rail.parse_date(rail.get_dag_run_conf()['enddate'], '%d/%m/%Y'),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        foreach_response_11 = rail.ForEachOperator(
            task_id='foreach_response_11',
            items="{{ result('get_time_off_details_for_user_and_date_range2_7') | to_json }}",
            start_task='insert_to_list_12',
            end_task='foreach_response_11_end'
        )

        insert_to_list_12 = rail.SetVariableOperator(
            task_id='insert_to_list_12',
            append=True,
            name='{{ result("declare_list_5").name }}',
            value={
                "timeoffid": "{{ result('foreach_response_11').customFields[0].text }}",
                "duration": "{{ result('foreach_response_11').startDateDetails.totalDuration.decimalWorkdays }}",
                "startdate": "{{ result('foreach_response_11').startDateDetails.date.day }}-{{ result('foreach_response_11').startDateDetails.date.month }}-{{ result('foreach_response_11').startDateDetails.date.year }}",
                "enddate": "{{ result('foreach_response_11').endDateDetails.date.day }}-{{ result('foreach_response_11').endDateDetails.date.month }}-{{ result('foreach_response_11').endDateDetails.date.year }}",
                "status": "{{ result('foreach_response_11').approvalStatus.displayText }}",
                "duration_hours": "{{ result('foreach_response_11').endDateDetails.totalDuration.calendarDayDuration.hours }}",
                "duratioin_mins": "{{ result('foreach_response_11').endDateDetails.totalDuration.calendarDayDuration.minutes }}",
                "timeoffuri": "{{ result('foreach_response_11').uri }}"
            }
        )

        foreach_response_11_end = rail.EmptyOperator(
            task_id='foreach_response_11_end',
        )

        if_plucktimeoffuri_smart_joinnil_present_15 = rail.IfOperator(
            task_id='if_plucktimeoffuri_smart_joinnil_present_15',
            test='''{{ result('insert_to_list_12') | is_truthy and (result('insert_to_list_12').value | find_first_by_attr_and_get_attr('timeoffid',dag_run.conf.AbsenceID)) | is_truthy }}''',
            yes_task="delete_timeoff_16",
            no_task="youviewtvlimited_timeoff_deletion_logs_add_entry_19",
        )

        delete_timeoff_16 = rail.RepliconServiceOperator(
            task_id='delete_timeoff_16',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('insert_to_list_12').value | find_first_by_attr_and_get_attr('timeoffid',dag_run.conf.AbsenceID,'timeoffuri') }}"
            }
        )

        youviewtvlimited_timeoff_deletion_logs_add_entry_17 = rail.WriteLogOperator(
            task_id='youviewtvlimited_timeoff_deletion_logs_add_entry_17',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{ dag_run.conf.Username }}",
                "absence#": "{{ dag_run.conf.AbsenceID }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "status": "Success",
                "details": "Time off booking deleted"
            }
        )

        youviewtvlimited_timeoff_deletion_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='youviewtvlimited_timeoff_deletion_logs_add_entry_19',
            log="{{ result('create_log') }}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{ dag_run.conf.Username }}",
                "absence#": "{{ dag_run.conf.AbsenceID }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "status": "Exception",
                "details": "Timeoff  not deleted since no booking found for the given Absence#"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.Username }}",
                "absence#": "{{ dag_run.conf.AbsenceID }}",
                "startdate": "{{ dag_run.conf.Startdate }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> declare_variable_4
        declare_variable_4 >> declare_list_5 >> create_log >> get_time_off_details_for_user_and_date_range2_7 >> foreach_response_11 >> insert_to_list_12 >> foreach_response_11_end
        foreach_response_11 >> foreach_response_11_end >> if_plucktimeoffuri_smart_joinnil_present_15
        if_plucktimeoffuri_smart_joinnil_present_15 >> rail.Label(
            'Yes') >> delete_timeoff_16 >> youviewtvlimited_timeoff_deletion_logs_add_entry_17 >> finish
        if_plucktimeoffuri_smart_joinnil_present_15 >> rail.Label(
            'No') >> youviewtvlimited_timeoff_deletion_logs_add_entry_19 >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
