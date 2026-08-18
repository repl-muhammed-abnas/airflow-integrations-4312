from datetime import datetime, timedelta
import uuid
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_timeoff_recal_no_batch_child_{config.instance}',
        description=f'Pwcfr_timeoff_recal_no_batch_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_schedulehours_equals_zero'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_schedulehours_equals_zero',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_schedulehours_equals_zero = rail.IfOperator(
            task_id='if_schedulehours_equals_zero',
            test=lambda dag_run: float(
                dag_run.conf['timeoff_batch_items']['schedulehrs']) == 0,
            yes_task='log_skipped_entries',
            no_task='log_startdate'
        )

        log_skipped_entries = rail.WriteLogOperator(
            task_id='log_skipped_entries',
            log="{{dag_run.conf.pwc_lookuptable}}",
            message="na",
            severity='Skipped',
            properties=lambda dag_run: {
                'user_name': dag_run.conf['timeoff_batch_items']['username'],
                'timeoff_type': dag_run.conf['timeoff_batch_items']['timeofftype'],
                'start_date': dag_run.conf['timeoff_batch_items']['bookingstartdate'],
                'booking_hours': dag_run.conf['timeoff_batch_items']['timeoffhrs'],
                'scheduled_hours': dag_run.conf['timeoff_batch_items']['schedulehrs'],
                'tracking_id': dag_run.conf['timeoff_batch_items']['customfieldtext'],
                'job_id': dag_run.conf['jobid'],
                'status': "Skipped",
                'reason': "Scheduled hours is 0"
            }
        )

        def get_startdate(dag_run):
            result = dag_run.conf['timeoff_batch_items']['bookingstartdate']
            date_obj = datetime.strptime(
                result, "%b %d, %Y")
            date_dict = {
                "Start_Day": date_obj.day,
                "Start_Month": date_obj.month,
                "Start_Year": date_obj.year
            }
            return date_dict

        log_startdate = rail.PythonOperator(
            task_id='log_startdate',
            python_callable=get_startdate
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetails2",
            data=lambda dag_run: {
                "timeOffUri": dag_run.conf['timeoff_batch_items']['bookinguri']
            }
        )

        if_displaytext_not_matches = rail.IfOperator(
            task_id='if_displaytext_not_matches',
            test="{{result('get_timeoff_details').timeOffStatus.displayText | lower in ('successful synchronization')}}",
            yes_task='reopen_timeoff_details',
            no_task='log_customfield'
        )

        reopen_timeoff_details = rail.RepliconServiceOperator(
            task_id='reopen_timeoff_details',
            endpoint="/services/TimeOffApprovalService1.svc/Reopen",
            data=lambda dag_run: {
                "timeOffUri": dag_run.conf['timeoff_batch_items']['bookinguri'],
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Reopened by Replicon Integration to adjust Time off hours"
            }
        )

        log_customfield = rail.PythonOperator(
            task_id='log_customfield',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_timeoff_details')['customFields'], 'customField.displayText', 'Time-off Tracking', 'customField.uri', null)
        )

        log_customfield_text = rail.PythonOperator(
            task_id='log_customfield_text',
            python_callable=lambda: rail.smartjoin_by_delim(
                rail.find_first_by_attr_and_get_attr(rail.result('get_timeoff_details')['customFields'], 'customField.displayText', 'Time-off Tracking', 'text', null), "") if rail.result('get_timeoff_details') else None
        )

        log_schedulehours = rail.PythonOperator(
            task_id='log_schedulehours',
            python_callable=lambda dag_run: dag_run.conf['timeoff_batch_items']['schedulehrs'] if dag_run.conf[
                'timeoff_batch_items']['timeoffcmts'] == 'D' else str(float(dag_run.conf['timeoff_batch_items']['schedulehrs'])/2)
        )

        def get_required_time():
            decimal_hours = float(rail.result('log_schedulehours'))
            hours = int(decimal_hours)
            minutes = int((decimal_hours - hours) * 60)
            seconds = int(((decimal_hours - hours) * 60 - minutes) * 60)
            time_format = {"Hours": hours,
                           "Minutes": minutes, "Seconds": seconds}
            return time_format

        convert_decimal_to_hours = rail.PythonOperator(
            task_id='convert_decimal_to_hours',
            python_callable=get_required_time
        )

        put_timeoff = rail.RepliconServiceOperator(
            task_id='put_timeoff',
            endpoint="/services/TimeOffService1.svc/PutTimeOff2",
            data=lambda dag_run: {
                    "timeOff": {
                        "target": {
                            "uri": dag_run.conf['timeoff_batch_items']['bookinguri'],
                        },
                        "owner": {
                            "uri": dag_run.conf['timeoff_batch_items']['useruri'],
                            "loginName": null,
                            "parameterCorrelationId": null
                        },
                        "timeOffType": {
                            "uri": dag_run.conf['timeoff_batch_items']['timeoffuri'],
                            "name": null
                        },
                        "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
                        "multiDayUsingStartEndDate": {
                            "timeOffStart": {
                                "date": {
                                    "year": rail.result('log_startdate')['Start_Year'],
                                    "month": rail.result('log_startdate')['Start_Month'],
                                    "day": rail.result('log_startdate')['Start_Day'],
                                },
                                "timeOfDay": null,
                                "relativeDuration": null,
                                "specificDuration": {
                                    "hours": rail.result('convert_decimal_to_hours')['Hours'],
                                    "minutes": rail.result('convert_decimal_to_hours')['Minutes'],
                                    "seconds": rail.result('convert_decimal_to_hours')['Seconds'],
                                    "milliseconds": "0",
                                    "microseconds": "0"
                                }
                            },
                            "timeOffEnd": {
                                "date": {
                                    "year": rail.result('log_startdate')['Start_Year'],
                                    "month": rail.result('log_startdate')['Start_Month'],
                                    "day": rail.result('log_startdate')['Start_Day'],
                                },
                                "timeOfDay": null,
                                "relativeDuration": null,
                                "specificDuration": {
                                    "hours": rail.result('convert_decimal_to_hours')['Hours'],
                                    "minutes": rail.result('convert_decimal_to_hours')['Minutes'],
                                    "seconds": rail.result('convert_decimal_to_hours')['Seconds'],
                                    "milliseconds": "0",
                                    "microseconds": "0"
                                }
                            }
                        },
                        "userExplicitEntries": [],
                        "comments": dag_run.conf['timeoff_batch_items']['timeoffcmts'],
                        "customFieldValues": [
                            {
                                "customField": {
                                    "uri": rail.result('log_customfield'),
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": rail.result('log_customfield_text'),
                                "date": null,
                                "dropDownOption": null,
                                "number": null
                            }
                        ]
                    }
            }
        )

        add_entry_in_force_timesheet_lookuptable = rail.WriteLogOperator(
            task_id='add_entry_in_force_timesheet_lookuptable',
            log="{{dag_run.conf.lookup_table}}",
            message='na',
            severity='',
            properties=lambda dag_run: {
                'jobid': dag_run.conf['parentjobid'],
                'username': dag_run.conf['timeoff_batch_items']['username'],
                'timeoffuri': rail.result('put_timeoff')['uri'],
                'workid': datetime.now().strftime("%S_%L"),
                'comments': "Time Off Transactions were recalculated due to a change in the user's schedule"

            }
        )

        add_success_entries = rail.WriteLogOperator(
            task_id='add_success_entries',
            log="{{dag_run.conf.pwc_lookuptable}}",
            message="na",
            severity='Success',
            properties=lambda dag_run: {
                'user_name': dag_run.conf['timeoff_batch_items']['username'],
                'time_off_type': dag_run.conf['timeoff_batch_items']['timeofftype'],
                'start_date': dag_run.conf['timeoff_batch_items']['bookingstartdate'],
                'status': "Success",
                'booking_hours': dag_run.conf['timeoff_batch_items']['timeoffhrs'],
                'schedule_hours': dag_run.conf['timeoff_batch_items']['schedulehrs'],
                'tracking_id': dag_run.conf['timeoff_batch_items']['customfieldtext'],
                'jobid': dag_run.conf['parentjobid'],
                'reason': "Time Off hours updated successfully"

            }
        )

        add_error_entries = rail.WriteLogOperator(
            task_id='add_error_entries',
            log="{{dag_run.conf.pwc_lookuptable}}",
            trigger_rule='one_failed',
            message="{{get_error_message()}}",
            severity='Error',
            properties=lambda dag_run: {
                'user_name': dag_run.conf['timeoff_batch_items']['username'],
                'time_off_type': dag_run.conf['timeoff_batch_items']['timeofftype'],
                'start_date': dag_run.conf['timeoff_batch_items']['bookingstartdate'],
                'status': "Error",
                'booking_hours': dag_run.conf['timeoff_batch_items']['timeoffhrs'],
                'schedule_hours': dag_run.conf['timeoff_batch_items']['schedulehrs'],
                'tracking_id': dag_run.conf['timeoff_batch_items']['customfieldtext'],
                'jobid': dag_run.conf['jobid'],
                'reason': "{{get_error_message()}}"

            }
        )

        end_job = rail.EmptyOperator(
            task_id='end_job'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_schedulehours_equals_zero
        if_schedulehours_equals_zero >> rail.Label(
            'Yes') >> log_skipped_entries >> end_job
        if_schedulehours_equals_zero >> rail.Label(
            'No') >> log_startdate >> get_timeoff_details >> if_displaytext_not_matches
        if_displaytext_not_matches >> rail.Label(
            'Yes') >> reopen_timeoff_details >> log_customfield
        if_displaytext_not_matches >> rail.Label(
            'No') >> log_customfield >> log_customfield_text >> log_schedulehours >> convert_decimal_to_hours
        convert_decimal_to_hours >> put_timeoff >> add_entry_in_force_timesheet_lookuptable
        add_entry_in_force_timesheet_lookuptable >> add_success_entries >> add_error_entries >> end_job >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
