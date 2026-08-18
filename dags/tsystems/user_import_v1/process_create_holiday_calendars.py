from tsystems.user_import_v1.utils import response_filters, custom_methods
from airflow.models import Variable
import rail

null = None

def create_add_holiday_calendar_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_holiday_calendar_child_dag_id,
        description="T-Systems Add Holiday Calendar Child DAG - Creates new holiday calendars in Replicon",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.create_holiday_calendar_child_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_holiday_calendars"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_holiday_calendars",
            end_task="catch_and_log_errors"
        )

        query_distinct_holiday_calendars = rail.QueryCollectionOperator(
            task_id="query_distinct_holiday_calendars",
            query="""SELECT DISTINCT 
                    country_of_employment || '_' || city_of_employment as location_combination
                    FROM valid_users_payload_data 
                    WHERE NULLIF(country_of_employment, '') IS NOT NULL 
                        AND NULLIF(city_of_employment, '') IS NOT NULL""",
            name="users_holiday_calendars"
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=response_filters.get_all_holiday_calendars,
            target="artifact"
        )

        create_existing_holiday_calendars_collections = rail.CreateCollectionOperator(
            task_id="create_existing_holiday_calendars_collections",
            source='{{ result("get_all_holiday_calendars") | load_all_records | to_json}}',
            name="existing_holiday_calendars"
        )

        query_distinct_existing_holiday_calendars = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_holiday_calendars",
            query="""SELECT DISTINCT holiday_calendar_name from existing_holiday_calendars""",
            name="distinct_existing_holiday_calendars"
        )

        query_new_holiday_calendars = rail.QueryCollectionOperator(
            task_id="query_new_holiday_calendars",
            query="""SELECT location_combination as holiday_calendar_name
                FROM users_holiday_calendars
                WHERE location_combination NOT IN (
                    SELECT holiday_calendar_name
                    FROM distinct_existing_holiday_calendars
            )"""
        )

        if_new_holiday_calendars = rail.IfOperator(
            task_id="if_new_holiday_calendars",
            test='{{ result("query_new_holiday_calendars", "length") > 0 }}',
            yes_task="put_holiday_calendar",
            no_task="holiday_calendar_end"
        )

        put_holiday_calendar = rail.RepliconServiceCallForEachItemOperator(
            task_id='put_holiday_calendar',
            endpoint="/services/HolidayCalendarService2.svc/PutHolidayCalendar",
            items='{{ result("query_new_holiday_calendars") }}',
            data=lambda item: {
                "calendar": {
                    "target": {
                        "uri": null,
                        "name": item["holiday_calendar_name"]
                    },
                    "name": item["holiday_calendar_name"],
                    "details": []
                }
            }
        )

        holiday_calendar_end = rail.EmptyOperator(task_id="holiday_calendar_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message="Holiday Calendar create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Holiday Calendar create failed - " + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_holiday_calendars >> get_all_holiday_calendars >> create_existing_holiday_calendars_collections >>\
            query_distinct_existing_holiday_calendars >> query_new_holiday_calendars >>\
            if_new_holiday_calendars >> rail.Label("Yes") >> put_holiday_calendar >>\
            holiday_calendar_end
        if_new_holiday_calendars >> rail.Label(
            "No") >> holiday_calendar_end >> catch_and_log_errors

        return dag

# Create child DAG for each instance
rail.for_each_instance(create_add_holiday_calendar_child_dag)
