from datetime import timedelta
from functools import lru_cache
import itertools
from pendulum import datetime
import rail

from dxctechnology.workday_user_import.user_import.tasks.get_all_required_data import get_all_required_fields
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_canada_user_process_conf
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_all_run_ids_callable

def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_canada_data_child_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        get_canada_data = rail.QueryCollectionOperator(
            task_id = "get_canada_data",
            # config.batch_count is number of batches the child has
            # for example:
            # for batch_count = 2
            # process_user_1, process_user_2 (same will be applicable for the add/update user)
            #! the record_modulo will be used at the later phase to trigger the appropriate child dag..
            #! ..for record processing, its necessary to have it same as how many child dags u want to keep,
            #! so edit the count with precautions
            #? splitter_batch added to get the trigger conf easily without relaying on external factors
            query=f"""SELECT
                    {"'COMPASS_canada'"} as splitter_batch_name,
                    ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,
                    ROW_NUMBER() OVER(ORDER BY ROWID)%{config.batch_count} as record_modulo,
                    sgugd.*,
                        (SELECT m."Source" FROM mapper m WHERE
                            m."Type" = "Company Code" AND
                            m.uri == sgugd.companycode LIMIT 1) as parent_company,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Office Schedule" LIMIT 1) as mapper_office_schedule,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Authentication" LIMIT 1) as mapper_authentication,
                        (SELECT m.uri FROM mapper m WHERE
                            m."Type" == "Authentication" LIMIT 1) as mapper_authentication_uri ,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Timesheet Approval" AND
                            m."Source" == "All" LIMIT 1) as mapper_timesheet_approval_path,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Timesheet Period" LIMIT 1) as mapper_timesheet_period,
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
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Schedule Type" AND
                            m.Country == "Default" LIMIT 1) as mapper_schedule_type,
                        (SELECT m.uri FROM mapper m WHERE
                            m."Type" == "Schedule Type" AND
                            m.Country == "Default" LIMIT 1) as mapper_schedule_type_uri,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "End User Permission Connect Employee" AND
                            m.Country ="Australia" LIMIT 1) as mapper_end_user_permission_connect_emp,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Timesheet Approval" AND
                            m."Source" ="C1" LIMIT 1) as mapper_timesheet_approval_path_canada,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Timeoff Approval" AND
                            m."Source" ="C1" LIMIT 1) as mapper_timeoff_approval_path_canada,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Supervisor Scheduler Permission" LIMIT 1) as mapper_supervisor_scheduler_permission,
                        (SELECT m.value FROM mapper m WHERE
                            m."Type" = "PSG" AND
                            m."Source" = "C1" AND
                            m.personnelsubarea = sgugd.areacode AND
                            m.employeegroup = sgugd.subareacode AND
                            m.status=sgugd.companycode LIMIT 1) as mapper_psg,
                        
                        CASE 
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m.Country = sgugd._country_to_use_for_query AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgugd.companycode LIMIT 1)) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m.Country = sgugd._country_to_use_for_query AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgugd.companycode LIMIT 1) LIMIT 1)
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgugd.companycode LIMIT 1)) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgugd.companycode LIMIT 1) LIMIT 1)
                            ELSE 
                                NULL
                        END as 'mapper_timeoff_template',
                        (SELECT m."Value" FROM mapper m WHERE
                            m."Type" == "Timeoff Template" AND
                            m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE
                                                m2."Type" = "Company Code" AND
                                                m2.uri == sgugd.companycode LIMIT 1
                                        )
                            LIMIT 1) as mapper_timeoff_template_name,
                        (SELECT m."Value" FROM mapper m WHERE
                            m."Type" == "Timeoff Approval" AND
                            m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE
                                                m2."Type" = "Company Code" AND
                                                m2.uri == sgugd.companycode LIMIT 1
                                        )
                            LIMIT 1) as mapper_timeoff_approval,
                        CASE 
                            WHEN (SELECT m2."Source" FROM mapper m2 WHERE
                                    m2."Type" = "Company Code" AND
                                    m2.uri == sgugd.companycode LIMIT 1) IS NOT NULL
                                THEN (SELECT m."Value" FROM mapper m WHERE
                                        m."Type" == "Time Entry Approval Path" AND
                                        m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE 
                                                            m2."Type" = "Company Code" AND
                                                            m2.uri == sgugd.companycode LIMIT 1
                                                    )
                                        LIMIT 1)
                            ELSE 
                                NULL
                        END as mapper_time_entry_approval_path_name,
                        CASE 
                            WHEN (SELECT m2.status FROM mapper m2 WHERE
                                    m2."Type" = "Company Code" AND
                                    m2.uri == sgugd.companycode LIMIT 1) IS NOT NULL
                                THEN (SELECT m2.status FROM mapper m2 WHERE
                                    m2."Type" = "Company Code" AND
                                    m2.uri == sgugd.companycode LIMIT 1)
                            ELSE 
                                "disabled"
                        END AS mapper_profile_status,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == 'TimeZone' AND
                            m.Country == sgugd._country_to_use_for_query AND
                            m."Source" ="C1" AND
                            m.personnelsubarea = sgugd.subareacode AND
                            m.employeegroup = sgugd.empgroupcode AND
                            m.employeesubgroup = sgugd.empsubgroupcode AND
                            m.status=sgugd.companycode LIMIT 1) AS mapper_canada_timezone,
                        (SELECT m.uri FROM mapper m WHERE
                            m."Type" == 'TimeZone' AND
                            m.Country == sgugd._country_to_use_for_query AND
                            m."Source" ="C1" AND
                            m.personnelsubarea = sgugd.subareacode AND
                            m.employeegroup = sgugd.empgroupcode AND
                            m.employeesubgroup = sgugd.empsubgroupcode AND
                            m.status=sgugd.companycode LIMIT 1) AS mapper_canada_timezone_uri,
                        (SELECT GROUP_CONCAT(m."Value", "|") FROM mapper m WHERE
                            m."Type" == 'Activities' AND
                            m.Country == "Canada" AND
                            m."Source" in ("C1") AND
                            m.personnelsubarea  == sgugd.subareacode AND
                            m.employeegroup  == sgugd.empgroupcode AND
                            m.employeesubgroup  == sgugd.empsubgroupcode AND
                            m.status == sgugd.companycode
                            ) as mapper_canada_activities,
                        (SELECT m.value FROM mapper m WHERE (
                                m."Type" == "Holiday Calendar" AND
                                m.Country == sgugd._country_to_use_for_query AND 
                                m."Source" == "C1" AND 
                                m.personnelsubarea  == sgugd.subareacode AND
                                m.employeegroup  == sgugd.empgroupcode AND
                                m.employeesubgroup  == sgugd.empsubgroupcode AND
                                m.status == sgugd.companycode
                            ) LIMIT 1) as mapper_canada_holiday_calendar,
                        (SELECT m.value FROM mapper m WHERE (
                                m."Type" == "Timesheet Template" AND
                                m.Country == sgugd._country_to_use_for_query AND 
                                m."Source" == "C1" AND 
                                m.personnelsubarea  == sgugd.subareacode AND
                                m.employeegroup  == sgugd.empgroupcode AND
                                m.employeesubgroup  == sgugd.empsubgroupcode AND
                                m.status == sgugd.companycode
                            ) LIMIT 1) as mapper_canada_timesheet_template,
                        (SELECT m.value FROM mapper m WHERE (
                                m."Type" == "Payrule" AND
                                m.Country == sgugd._country_to_use_for_query AND 
                                m."Source" == "C1" AND 
                                m.personnelsubarea  == sgugd.subareacode AND
                                m.employeegroup  == sgugd.empgroupcode AND
                                m.employeesubgroup  == sgugd.empsubgroupcode AND
                                m.status == sgugd.companycode
                            ) LIMIT 1) as mapper_canada_payrule,
                        (SELECT m.value FROM mapper m WHERE (
                                m."Type" == "Timesheet Period Effective Date" AND
                                m.Country == sgugd._country_to_use_for_query
                            ) LIMIT 1) as mapper_canada_timesheet_period_effective_date,
                        (SELECT m.value FROM mapper m WHERE (
                                m."Type" == "TimeZone" AND
                                m.Country == sgugd._country_to_use_for_query
                            ) LIMIT 1) as mapper_timezone,
                        (SELECT m.uri FROM mapper m WHERE (
                                m."Type" == "TimeZone" AND
                                m.Country == sgugd._country_to_use_for_query
                            ) LIMIT 1) as mapper_timezone_uri,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "WorkWeek" AND m."Source" == (
                                SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1))
                            ELSE 
                                NULL
                        END as mapper_work_week,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                THEN (SELECT m."uri" FROM mapper m WHERE m."Type" == "WorkWeek" AND m."Source" == (
                                SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1))
                            ELSE 
                                NULL
                        END as mapper_work_week_uri,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                THEN 
                                    CASE
                                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) == "COMPASS"
                                            THEN (SELECT GROUP_CONCAT(m."value", "|") FROM mapper m WHERE m."Type"= "Activities" AND m."Source" == "COMPASS")
                                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) == "FTP"
                                            THEN (SELECT GROUP_CONCAT(m."value", "|") FROM mapper m WHERE m."Type"= "Activities" AND m."Source" == "FTP")
                                        ELSE 
                                            NULL
                                    END
                            ELSE 
                                NULL
                        END as mapper_activities,
                        (SELECT m."value" FROM mapper m WHERE
                            m."Type" == "Holiday Calendar" AND
                            m.Country == sgugd.homecountry AND 
                            m."Source" == (
                                SELECT m."Source" FROM mapper m WHERE
                                    m."Type" == "Company Code" AND
                                    m.uri == sgugd."companycode" LIMIT 1
                                )
                            LIMIT 1) AS mapper_holiday_calendar,
                        CASE
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) == "C1"
                                THEN  sgugd.subareacode || ' | ' || sgugd.empgroupcode || ' | ' || sgugd.empsubgroupcode
                            WHEN sgugd.exempt == "Yes"
                                THEN "Exempt – Salaried"
                            ELSE 
                                "Non Exempt - Hourly"
                        END as employee_type_fullpath,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                THEN 
                                    CASE
                                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) == "COMPASS"
                                            THEN (SELECT GROUP_CONCAT(m."value", "|") FROM mapper m WHERE m."Type"= "Activities" AND m.Country = "Others" AND m."Source" == "COMPASS")
                                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) == "FTP"
                                            THEN (SELECT GROUP_CONCAT(m."value", "|") FROM mapper m WHERE m."Type"= "Activities" AND m."Source" == "FTP")
                                        ELSE 
                                            NULL
                                    END
                            ELSE 
                                NULL
                        END as mapper_non_usa_pri_ind_prt_cri_aus_countries_activities,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                THEN 
                                    (SELECT m."Source" FROM mapper m 
                                            WHERE m."Type" == "Holiday Calendar" 
                                            AND m.Country == sgugd._country_to_use_for_query 
                                            AND m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) LIMIT 1) == "COMPASS"
                            ELSE 
                                NULL
                        END as mapper_non_usa_pri_ind_prt_cri_aus_countries_holidaycalander,
                        CASE 
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Country to enable" AND m.Country == sgugd._country_to_use_for_query LIMIT 1) IS NOT NULL
                                THEN 
                                    CASE 
                                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                            THEN 
                                                "Enable"
                                            ELSE 
                                                Null
                                    END 
                            ELSE 
                                NULL
                        END as mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country,
                        (SELECT m."Source" FROM mapper m 
                            WHERE m."Type" == "Holiday Calendar" 
                                AND m.Country == sgugd._country_to_use_for_query AND m.Source == "C1" 
                                AND m.personnelsubarea == sgugd._country_to_use_for_query LIMIT 1) as mapper_prt_not_compass_holidaycalander_canada_holidaycalendar,
                        (SELECT m."Value" FROM mapper m
                            WHERE m."type" == "Company Code"
                                AND m."URI" == sgugd."companycode") as company_code_full_path,
                        (SELECT m."Value" FROM mapper m
                            WHERE m."type" == "Timesheet Period Effective Date"
                                AND m."Country" == sgugd._country_to_use_for_query) as timesheet_period_effective_date,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1) IS NOT NULL
                                THEN 
                                    (SELECT m."value" FROM mapper m 
                                    WHERE m."Type" = "Timesheet Template"
                                    AND m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1))
                            ELSE 
                                (SELECT m."value" FROM mapper m 
                                WHERE m."Type" = "Timesheet Template"
                                AND m."Source" == "No Timesheet")
                        END as mapper_timesheet_template,
                    (SELECT GROUP_CONCAT(m."value", "```") FROM mapper m 
                        WHERE m."Type" == 'Timeoff' 
                        AND m."Country" = "ALL" 
                        AND m."Function" = "Workday User Sync" 
                        AND m."Source"==(SELECT m."Source" FROM mapper m WHERE m."Type" == "Company Code" AND m.uri == sgugd."companycode" LIMIT 1)) as mapper_timeoffs,
                    (SELECT m."value" FROM mapper m 
                        WHERE m."Type" == 'Supervisor Permission Connect Employee' 
                        AND m."Country" = "Australia" 
                        AND m."Function" = "Workday User Sync" 
                    ) as mapper_aus_supervisor_end_user_permission,
                    (SELECT m."value" FROM mapper m 
                        WHERE m."Type" == 'Timesheet Period' 
                        AND m."Country" = sgugd._country_to_use_for_query
                        AND m."Source" = "C1" 
                    ) as mapper_timesheet_period_canada
		            FROM c1_canada_data sgugd"""
        )

        start_task, end_task = get_all_required_fields("canada_get_all_records", config)

        @lru_cache(maxsize=8)
        def items():
            return rail.load_all_records(rail.result(get_canada_data.task_id))

        trigger_process_user = rail.trigger_parallel_dagrun(
            task_id = "trigger_process_user",
            items=items,
            trigger_dag_id=config.workday_user_import_process_canada_users_child_dag,
            parallel_count=config.process_users_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf = lambda item, dag_run : get_canada_user_process_conf(item, dag_run, config)
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

        get_canada_data >> start_task
        end_task >> trigger_process_user >> get_all_run_ids >> gather_all_logs

        return dag
    
rail.for_each_instance(create_dag)
