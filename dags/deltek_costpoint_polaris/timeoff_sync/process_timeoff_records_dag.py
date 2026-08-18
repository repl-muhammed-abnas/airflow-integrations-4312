from datetime import timedelta
import uuid
import pendulum
from datetime import datetime
from airflow.models import Variable
import rail


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_process_timeoff_records_child_{config.instance}',
        description=f'deltek_costpoint_process_timeoff_records_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
        },
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
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log",
        )

        def add_timeoff(dag_run):
            if dag_run.conf['timeoff_type'].lower() == config.cp_holiday_name.lower():
                if config.create_holiday_as_timeoff is True:
                    return True
                else:
                    return False
            return True

        is_holiday_as_timeoff = rail.IfOperator(
            task_id='is_holiday_as_timeoff',
            test=add_timeoff,
            yes_task="if_timeoff_not_present",
            no_task="finish",
        )

        if_timeoff_not_present = rail.IfOperator(
            task_id='if_timeoff_not_present',
            test='''{{ dag_run.conf.bookinguri | is_falsy }}''',
            yes_task="put_and_submit_time_off",
            no_task="if_timeoff_deleted",
        )

        if_timeoff_deleted = rail.IfOperator(
            task_id='if_timeoff_deleted',
            test='''{{ dag_run.conf.deletedtimeoff | is_truthy }}''',
            yes_task="delete_time_off",
            no_task="if_timeoff_open",
        )

        if_timeoff_open = rail.IfOperator(
            task_id='if_timeoff_open',
            test='''{{ dag_run.conf.istimeoffinopenstatus | is_truthy }}''',
            yes_task="put_and_submit_time_off",
            no_task="reopen_and_put_timeoff",
        )

        delete_time_off = rail.RepliconServiceOperator(
            task_id='delete_time_off',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ dag_run.conf.bookinguri }}"
            }
        )

        put_and_submit_time_off = rail.RepliconServiceOperator(
            task_id="put_and_submit_time_off",
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            retries = 0,
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": dag_run.conf['bookinguri']
                        if dag_run.conf['bookinguri'] else null
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).year,
                                "month": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).month,
                                "day": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": int(float(dag_run.conf['timeoffhours'])),
                                "minutes": int((float(dag_run.conf['timeoffhours']) * 60) % 60),
                                "seconds": int((float(dag_run.conf['timeoffhours']) * 3600) % 60),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).year,
                                "month": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).month,
                                "day": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).day
                            },
                            "timeOfDay": null,
                            "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                            "specificDuration": null
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "",
                    "customFieldValues": [],
                    "objectExtensionFieldValues": []
                },
                "comments": "Timeoff from Cosptpoint",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        reopen_and_put_timeoff = rail.RepliconServiceOperator(
            task_id="reopen_and_put_timeoff",
            retries = 0,
            endpoint="services/TimeOffApprovalService1.svc/ReopenPutAndSubmitTimeOff3",
            data=lambda dag_run: {
                "timeOff": {
                    "target": {
                        "uri": dag_run.conf['bookinguri']
                    },
                    "owner": {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "timeOffType": {
                        "uri": dag_run.conf['timeoffuri'],
                        "name": null
                    },
                    "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                    "multiDayUsingStartEndDate": {
                        "timeOffStart": {
                            "date": {
                                "year": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).year,
                                "month": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).month,
                                "day": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": {
                                "hours": int(float(dag_run.conf['timeoffhours'])),
                                "minutes": int((float(dag_run.conf['timeoffhours']) * 60) % 60),
                                "seconds": int((float(dag_run.conf['timeoffhours']) * 3600) % 60),
                                "milliseconds": "0",
                                "microseconds": "0"
                            }
                        },
                        "timeOffEnd": {
                            "date": {
                                "year": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).year,
                                "month": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).month,
                                "day": datetime.strptime(
                                    dag_run.conf['timeoffdate'], config.costpoint_to_date_format).day
                            },
                            "timeOfDay": null,
                            "relativeDuration": null,
                            "specificDuration": null
                        }
                    },
                    "userExplicitEntries": [],
                    "comments": "",
                    "customFieldValues": []
                },
                "comments": "Timeoff from Cosptpoint",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        add_log_entry = rail.WriteLogOperator(
            task_id='add_log_entry',
            log="{{ result('create_log') }}",
            message="na",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.empid }}",
                "timeoff":  "Time Off",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "action": "{{ 'Add'  if dag_run.conf.bookinguri | is_falsy else 'Update' }}",
                "status": "Success",
                "details": "",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.empid }}",
                "timeoff":  "Time Off",
                "timeoffdate": "{{ dag_run.conf.timeoffdate }}",
                "action": "{{ 'Add'  if dag_run.conf.bookinguri | is_falsy else 'Update' }}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log
        create_log >> is_holiday_as_timeoff
        is_holiday_as_timeoff >> rail.Label('No') >> finish
        is_holiday_as_timeoff >> rail.Label('Yes') >> if_timeoff_not_present
        if_timeoff_not_present >> rail.Label(
            'No') >> if_timeoff_deleted
        if_timeoff_deleted >> rail.Label('No') >> if_timeoff_open
        if_timeoff_open >> rail.Label(
            'No') >> reopen_and_put_timeoff >> add_log_entry >> finish >> catch_and_log_error >> log_to_sumo
        if_timeoff_open >> rail.Label(
            'Yes') >> put_and_submit_time_off >> add_log_entry
        if_timeoff_deleted >> rail.Label('Yes') >> delete_time_off >> finish
        if_timeoff_not_present >> rail.Label(
            'Yes') >> put_and_submit_time_off

    return dag


rail.for_each_instance(create_dag)
