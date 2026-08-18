from datetime import timedelta
from functools import lru_cache
import itertools
from pendulum import datetime
import rail
from airflow.models import Variable

from dxctechnology.workday_user_import_v1.user_import.tasks.get_all_required_data import get_all_required_fields
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_portugal_user_process_conf
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import get_all_run_ids_callable

def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_portugal_data_child_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        get_portugal_data = rail.QueryCollectionOperator(
            task_id = "get_portugal_data",
            # config.batch_count is number of batches the child has
            # for example:
            # for batch_count = 2
            # process_user_1, process_user_2 (same will be applicable for the add/update user)
            #! the record_modulo will be used at the later phase to trigger the appropriate child dag..
            #! ..for record processing, its necessary to have it same as how many child dags u want to keep,
            #! so edit the count with precautions
            #? splitter_batch added to get the trigger conf easily without relaying on external factors
            query=f"""SELECT
                    {"'COMPASS_PORTUGAL'"} as splitter_batch_name,
                    ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,
                    ROW_NUMBER() OVER(ORDER BY ROWID)%{config.batch_count} as record_modulo,
                    cpd.*,
                    (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) as parent_company_code,
                    (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Office Schedule" LIMIT 1) as mapper_office_schedule,
                    (SELECT m.Value FROM mapper m WHERE
                        m."Type" == "Authentication" LIMIT 1) as mapper_authentication,
                    (SELECT m.uri FROM mapper m WHERE
                        m."Type" == "Authentication" LIMIT 1) as mapper_authentication_uri,
                    (SELECT m.Value FROM mapper m WHERE
                        m."Type" == "End User Permission" LIMIT 1) as mapper_end_user_permission,
                    (SELECT m.Value FROM mapper m WHERE
                        m."Type" == "Supervisor User Permission" LIMIT 1) as mapper_supervisor_user_permission,
                    (SELECT GROUP_CONCAT(m.Value, "|") FROM mapper m WHERE
                        m."Type" == "Product") as mapper_product,
                    (SELECT GROUP_CONCAT(m.uri, "|") FROM mapper m WHERE
                        m."Type" == "Product") as mapper_product_uri,
                    (SELECT m.Value FROM mapper m WHERE
                        m."Type" == "Language" LIMIT 1) as mapper_language,
                    (SELECT m.uri FROM mapper m WHERE
                        m."Type" == "Language" LIMIT 1) as mapper_language_uri,
                    (SELECT m.Value FROM mapper m WHERE
                        m."Type" == "Supervisor End User Permission" LIMIT 1) as mapper_supervisor_end_user_permission,
                    (SELECT m."value" FROM mapper m 
                        WHERE m."Type" == 'Supervisor Permission Connect Employee' 
                        AND m."Country" = "Australia" 
                        AND m."Function" = "Workday User Sync" 
                    ) as mapper_aus_supervisor_end_user_permission,
                    (SELECT m.Value FROM mapper m WHERE
                        m."Type" == "End User Permission Connect Employee" AND
                        m.Country ="Australia" LIMIT 1) as mapper_end_user_permission_connect_emp,
                    (SELECT m.uri FROM mapper m WHERE
                            m."Type" == "Schedule Type" AND
                            m.Country == "Default" LIMIT 1) as mapper_default_schedule_type_uri,
                    CASE 
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m.Country = cpd._country_to_use_for_query AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == cpd.companycode LIMIT 1)) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m.Country = cpd._country_to_use_for_query AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == cpd.companycode LIMIT 1) LIMIT 1)
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == cpd.companycode LIMIT 1)) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == cpd.companycode LIMIT 1) LIMIT 1)
                            ELSE 
                                NULL
                        END as 'mapper_timeoff_template_master',
                    CASE 
                            WHEN (SELECT m."status" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == cpd.companycode LIMIT 1) IS NOT NULL
                                THEN
                                    (SELECT m."status" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == cpd.companycode LIMIT 1)
                            ELSE 
                                "disabled"
                    END as mapper_profile_status,
                    CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == cpd.companycode LIMIT 1) IS NOT NULL
                                THEN
                                    (SELECT m."value" FROM mapper m WHERE m."Type" = "Time Entry Approval Path" AND
                                        m."source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == cpd.companycode LIMIT 1)
                                    )
                            ELSE 
                                NULL 
                    END as mapper_timeentry_approval_path,
                    CASE
                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == cpd."companycode" LIMIT 1) == "C1"
                            THEN  cpd.subareacode || ' | ' || cpd.empgroupcode || ' | ' || cpd.empsubgroupcode
                        WHEN cpd.exempt == "Yes"
                            THEN "Exempt – Salaried"
                        ELSE 
                            "Non Exempt - Hourly"
                    END as employee_type_fullpath,
                    CASE 
                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == cpd."companycode" LIMIT 1) IS NOT NULL
                            THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "WorkWeek" AND m."Source" == (
                                    SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == cpd."companycode" LIMIT 1))
                        ELSE 
                            NULL
                    END as mapper_work_week,
                    CASE 
                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == cpd."companycode" LIMIT 1) IS NOT NULL
                            THEN (SELECT m."uri" FROM mapper m WHERE m."Type" == "WorkWeek" AND m."Source" == (
                                    SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == cpd."companycode" LIMIT 1))
                        ELSE 
                            NULL
                    END as mapper_work_week_uri,
                    (SELECT m."Value" FROM mapper m
                            WHERE m."type" == "Company Code"
                                AND m."URI" == cpd."companycode") as company_code_full_path,
                    (SELECT m."Value" FROM mapper m WHERE 
                        m."Type" == "Timesheet Approval" AND
                        m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode) AND 
                        m.Country == cpd._country_to_use_for_query AND 
                        m.personnelsubarea == CASE 
                                WHEN cpd.workshift LIKE "BPSOT%"
                                    THEN "BPSOT"
                                ELSE
                                    CASE 
                                        WHEN cpd.workshift LIKE "BPS%"
                                            THEN "BPS"
                                        ELSE 
                                            cpd.workshift
                                    END
                            END AND
                        LOWER(m.employeegroup) == LOWER(cpd.gender) LIMIT 1
                    ) as mapper_timesheet_approval_path_portugal_compass,
                    (
                        SELECT m."value" FROM mapper m WHERE 
                            m."Type" == "Timesheet Template" AND 
                            m."Source" == CASE
                                    WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) IS NOT NULL
                                        THEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1)
                                    ELSE 
                                        "No Timesheet"
                                END AND
                            m.Country == cpd._country_to_use_for_query AND 
                            m.personnelsubarea == CASE 
                                WHEN cpd.workshift LIKE "BPSOT%"
                                    THEN "BPSOT"
                                ELSE
                                    CASE 
                                        WHEN cpd.workshift LIKE "BPS%"
                                            THEN "BPS"
                                        ELSE 
                                            cpd.workshift
                                    END
                            END AND
                        LOWER(m.employeegroup) == LOWER(cpd.gender) LIMIT 1
                    ) as mapper_timesheet_template_portugal_compass,
                    -- Country can be done while calling the trigger
                    (
                        SELECT m."value" FROM mapper m WHERE 
                            m."Type" == "Timeoff Template" AND 
                            m."Country" == cpd._country_to_use_for_query AND
                            m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) 
                    ) as mapper_timeoff_template_portugal_compass,
                    (
                        SELECT m."value" FROM mapper m WHERE 
                            m."Type" == "Timeoff Approval" AND 
                            m."Country" == cpd._country_to_use_for_query AND
                            m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) 
                    ) as mapper_timeoff_approval_portugal_compass,
                    (
                        SELECT m."value" FROM mapper m WHERE 
                            m."Type" == "Timesheet Period" AND 
                            m."Country" == cpd._country_to_use_for_query AND
                            m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1)
                    ) as mapper_timesheet_period_portugal_compass,
                    CASE 
                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.URI == cpd.companycode LIMIT 1) IS NOT NULL
                            THEN CASE 
                                WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) == "COMPASS"
                                    THEN (SELECT GROUP_CONCAT(m3."value", "|") FROM mapper m3 WHERE
                                        m3."Type" == "Activities" AND
                                        m3."country" == cpd._country_to_use_for_query AND
                                        m3."source" == "COMPASS" AND
                                        m3.personnelsubarea == CASE
                                                WHEN  LOWER(cpd.workshift) LIKE "%ot%"
                                                    THEN "OT"
                                                ELSE
                                                    "Non-OT"
                                            END
                                        )
                                ELSE 
                                    NULL
                            END
                        ELSE
                            NULL
                    END mapper_activity_list__portugal_compass,
                    (SELECT m."value" FROM mapper m WHERE m."type" == "TimeZone" AND m."country" == cpd._country_to_use_for_query) AS mapper_timezone_portugal_compass,
                    (SELECT m."uri" FROM mapper m WHERE m."type" == "TimeZone" AND m."country" == cpd._country_to_use_for_query) AS mapper_timezone_uri_portugal_compass,
                    (SELECT m."value" FROM mapper m WHERE 
                        m."type" == "Holiday Calendar" AND
                        m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) AND
                        m."country" == cpd._country_to_use_for_query) AS mapper_holiday_calendar_portugal_compass,
                    CASE 
                        WHEN (SELECT m."value" FROM mapper m WHERE m."type" == "Country to enable" AND m.country == cpd._country_to_use_for_query LIMIT 1) IS NOT NULL
                            THEN CASE 
                                WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) IS NOT NULL
                                    THEN "Enable"
                                ELSE 
                                    NULL
                            END
                        ELSE 
                            NULL 
                    END as mapper_country_to_enable_portugal_compass,
                    (SELECT m."value" FROM mapper m WHERE m."type" == "Timesheet Period Effective Date" AND m.country = cpd._country_to_use_for_query LIMIT 1) AS mapper_timesheet_period_effective_date_portugal_compass,
                    (SELECT m."value" FROM mapper m WHERE m."type" == "Payrule" AND
                        m."Source" == CASE
                            WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) IS NOT NULL
                                THEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1)
                            ELSE 
                                "No Payrule"
                        END AND 
                        m.country == cpd._country_to_use_for_query AND 
                        m.personnelsubarea == CASE 
                                WHEN cpd.workshift LIKE "BPSOT%"
                                    THEN "BPSOT"
                                ELSE
                                    CASE 
                                        WHEN cpd.workshift LIKE "BPS%"
                                            THEN "BPS"
                                        ELSE 
                                            cpd.workshift
                                    END
                            END AND
                        LOWER(m.employeegroup) == LOWER(cpd.gender) LIMIT 1
                    ) as mapper_payrule_portugal_compass,
                    (
                        SELECT m."value" FROM mapper m WHERE 
                            m."type" == "Punch Entry Policy" AND
                            m."source" == CASE
                                WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) IS NOT NULL
                                    THEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1)
                                ELSE 
                                    "No Punch Entry"
                            END AND
                            m.country == cpd._country_to_use_for_query AND 
                            m.personnelsubarea == CASE 
                                    WHEN cpd.workshift LIKE "BPSOT%"
                                        THEN "BPSOT"
                                    ELSE
                                        CASE 
                                            WHEN cpd.workshift LIKE "BPS%"
                                                THEN "BPS"
                                            ELSE 
                                                cpd.workshift
                                        END
                                END AND
                            LOWER(m.employeegroup) == LOWER(cpd.gender) LIMIT 1
                    ) as mapper_punch_entry_policy_portugal_compass,
                    (
                        SELECT m."value" FROM mapper m WHERE 
                            m."type" == "Schedule" AND
                            m."source" == CASE
                                WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1) IS NOT NULL
                                    THEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" == "Company Code" AND m2.URI == cpd.companycode LIMIT 1)
                                ELSE 
                                    "No Schedule"
                            END AND
                            m.country == cpd._country_to_use_for_query AND 
                            m.personnelsubarea == CASE 
                                    WHEN cpd.workshift LIKE "BPSOT%"
                                        THEN "BPSOT"
                                    ELSE
                                        CASE 
                                            WHEN cpd.workshift LIKE "BPS%"
                                                THEN "BPS"
                                            ELSE 
                                                cpd.workshift
                                        END
                                END AND
                            LOWER(m.employeegroup) == LOWER(cpd.gender) LIMIT 1
                    ) as mapper_schedule_name_portugal_compass,
                    (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Supervisor Scheduler Permission" LIMIT 1) as mapper_supervisor_scheduler_permission
                FROM compass_portugal_data cpd"""
        )

        start_task, end_task = get_all_required_fields("portugal_get_all_records", config)

        @lru_cache(maxsize=8)
        def items():
            return rail.load_all_records(rail.result(get_portugal_data.task_id))

        trigger_process_user = rail.trigger_parallel_dagrun(
            task_id = "trigger_process_user",
            items=items,
            trigger_dag_id=config.workday_user_import_portugal_process_users_child_dag,
            parallel_count=config.process_users_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf = lambda item, dag_run : get_portugal_user_process_conf(item, dag_run, config)
        )

        get_all_run_ids = rail.PythonOperator(
            task_id = "get_all_run_ids",
            python_callable = lambda: get_all_run_ids_callable('trigger_process_user', config.process_users_parallel_count),
        )

        gather_all_logs = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_all_logs",
            dagrun_task_id = "create_user_log",
            dag_runs="{{result('get_all_run_ids')}}"
        )

        get_portugal_data >> start_task
        end_task >> trigger_process_user >> get_all_run_ids >> gather_all_logs

        return dag
    
rail.for_each_instance(create_dag)
