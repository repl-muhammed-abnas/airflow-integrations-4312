from datetime import timedelta
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_holiday_sync_child_{config.instance}',
        description=f'deltek_costpoint_holiday_sync_{config.instance}',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_modified_holidays'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_modified_holidays',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def decimal_hours_to_timespan(decimal_hours):
            # Calculate the total number of seconds
            total_seconds = int(decimal_hours * 3600)

            # Extract hours, minutes, and seconds
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60

            return {
                "hours": hours,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": "0",
                "microseconds": "0"
            }

        get_modified_holidays = rail.PythonOperator(
            task_id='get_modified_holidays',
            python_callable=lambda: get_holidays_in_schedule()
        )

        def get_holidays_in_schedule():
            schedule = rail.get_dag_run_conf()['item']
            return schedule["holidays"]

        if_holiday_calendar_exists_in_polaris = rail.IfOperator(
            task_id='if_holiday_calendar_exists_in_polaris',
            test=lambda: get_action() == "Update",
            yes_task="get_polaris_holidays",
            no_task="put_holiday_calendar",
        )

        put_holiday_calendar = rail.RepliconServiceOperator(
            task_id='put_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/PutHolidayCalendar",
            data=lambda: {
                "calendar": {
                    "target": {
                        "uri": null,
                        "name": rail.get_dag_run_conf()['item']["name"]
                    },
                    "name": rail.get_dag_run_conf()['item']["name"],
                    "details": []
                }
            }
        )

        get_polaris_holidays = rail.RepliconServiceOperator(
            task_id='get_polaris_holidays',
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data=lambda: {
                "holidayCalendarUri": get_holiday_calendar_uri(),
                "dateRange": get_holiday_date_range()
            }
        )

        def get_holiday_date_range():
            schedule = rail.get_dag_run_conf()['item']
            
            holidays = schedule['holidays']
            
            if holidays and holidays[0]:
                startDate = holidays[0]["date"]
                startDate = rail.parse_date(startDate, config.date_time_format)
                endDate = holidays[-1]["date"]
                endDate = rail.parse_date(endDate, config.date_time_format)
                return {
                    "startDate": startDate,
                    "endDate": endDate,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }

        def get_holiday_calendar_uri():
            action = get_action()
            if action == "Update":
                existing_holidays = rail.get_dag_run_conf()['allHolidays']
                calendar_uri = rail.find_first_by_attr_and_get_attr(
                    existing_holidays, "name", rail.get_dag_run_conf()['item']['name'], "uri", None)
                return calendar_uri
            else:
                return rail.result("put_holiday_calendar")["uri"]

        def get_action():
            existing_holidays = rail.get_dag_run_conf()['allHolidays']
            item = rail.get_dag_run_conf()['item']
            holiday_info = list(
                filter(lambda x: x['displayText'] == item["name"], existing_holidays))
            return "Update" if holiday_info else "Add"

        foreach_holiday_date_flow = rail.ForEachOperator(
            task_id='foreach_holiday_date_flow',
            items="{{ result('get_modified_holidays') | to_json }}",
            start_task='create_update_holiday_in_polaris',
            end_task='foreach_holiday_date_flow_end'
        )

        foreach_holiday_date_flow_end = rail.EmptyOperator(
            task_id='foreach_holiday_date_flow_end',
        )

        create_update_holiday_in_polaris = rail.RepliconServiceOperator(
            task_id='create_update_holiday_in_polaris',
            endpoint="/services/HolidayCalendarService2.svc/PutHoliday2",
            data=lambda: {
                "holiday": {
                    "target": get_holiday_target(),
                    "calendar": {
                        "uri": null,
                        "name": get_calendar_name()
                    },
                    "name": "Deltek Costpoint Holiday",
                    "date": get_holiday_date(),
                    "durationTypeUri": "urn:replicon:time-off-relative-duration:full-day",
                    "duration": get_duration()
                },
                "holidayModificationOptionUri": "urn:replicon:holiday-modification-option:save"
            }
        )

        def get_calendar_name():
            return rail.get_dag_run_conf()['item']["name"]

        def get_holiday_target():
            polaris_holidays = rail.result('get_polaris_holidays')
            date = get_holiday_date()
            # polaris_holidays = config.polarisHolidays
            if polaris_holidays:
                for holiday in polaris_holidays:
                    if is_date_equal(holiday['date'], date):
                        return {
                            "uri": holiday["uri"]
                        }
            return null

        def is_date_equal(polarisDate, cpDate):
            return polarisDate["year"] == cpDate["year"] and polarisDate["month"] == cpDate["month"] \
                and polarisDate["day"] == cpDate["day"]

        def get_holiday_date():
            item = rail.result('foreach_holiday_date_flow')
            return rail.parse_date(item["date"], config.date_time_format)

        def get_duration():
            item = rail.result('foreach_holiday_date_flow')
            return decimal_hours_to_timespan(item['hours'])

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_modified_holidays >> if_holiday_calendar_exists_in_polaris

        if_holiday_calendar_exists_in_polaris >> rail.Label(
            'No') >> put_holiday_calendar >> foreach_holiday_date_flow

        if_holiday_calendar_exists_in_polaris >> rail.Label(
            'Yes') >> get_polaris_holidays >> foreach_holiday_date_flow

        foreach_holiday_date_flow >> create_update_holiday_in_polaris >> foreach_holiday_date_flow_end >> log_to_sumo

        foreach_holiday_date_flow >> foreach_holiday_date_flow_end

        return dag


rail.for_each_instance(create_dag)
