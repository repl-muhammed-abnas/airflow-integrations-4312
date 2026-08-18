from datetime import timedelta
from functools import lru_cache
import itertools
from pendulum import datetime
import rail

from dxctechnology.workday_user_import.user_import.tasks.get_all_required_data import get_all_required_fields
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_gsap_user_process_conf
from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import get_all_run_ids_callable

def create_dag(config):
    #! TO-DO later: 
    # Updates that can be done on this
    # 1. adding for loop which will cerate n number of dags,
    # where n is number of batches (C1, Compass, GSAP, Gbl, Non-live)
    #? Update the logic that allows to have the dag_batches differently per splitter batch (C1, CMPS, GSAP, Gbl)
    # Done via list of dict
    # 2. The trigger_conf will also be need to updated as per the splitter batch

    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_process_gsap_data_child_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        get_gsap_data = rail.QueryCollectionOperator(
            task_id = "get_gsap_data",
            # config.batch_count is number of batches the child has
            # for example:
            # for batch_count = 2
            # process_user_1, process_user_2 (same will be applicable for the add/update user)
            #! the record_modulo will be used at the later phase to trigger the appropriate child dag..
            #! ..for record processing, its necessary to have it same as how many child dags u want to keep,
            #! so edit the count with precautions
            #? splitter_batch added to get the trigger conf easily without relaying on external factors

            query=f"""SELECT 
                        {"'GSAP'"} as splitter_batch_name,
                        sgad.*,
                        (SELECT m."Source" FROM mapper m WHERE
                            m."Type" = "Company Code" AND
                            m.uri == sgad.companycode LIMIT 1) as parent_company,
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
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Schedule Type" AND
                            m.Country == "Default" LIMIT 1) as mapper_default_schedule_type,
                        (SELECT m.uri FROM mapper m WHERE
                            m."Type" == "Schedule Type" AND
                            m.Country == "Default" LIMIT 1) as mapper_default_schedule_type_uri,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "End User Permission Connect Employee" AND
                            m.Country ="Australia" LIMIT 1) as mapper_end_user_permission_connect_emp,
                        (SELECT m."Source" FROM mapper m WHERE
                            m."Type" = "Company Code" AND
                            m.uri == sgad.companycode LIMIT 1) as parent_company,
                        (SELECT m."Value" FROM mapper m
                            WHERE m."type" == "Company Code"
                                AND m."URI" == sgad."companycode") as company_code_full_path,
                        (SELECT m."value" FROM mapper m 
                            WHERE m."Type" == 'Supervisor Permission Connect Employee' 
                            AND m."Country" = "Australia" 
                            AND m."Function" = "Workday User Sync" 
                        ) as mapper_aus_supervisor_end_user_permission,
                        (SELECT m.value FROM mapper m 
                            WHERE m."Type" == "Schedule Type"
                            AND m.country == sgad._country_to_use_for_query 
                            AND m."Source" == sgad.workshift
                        ) as mapper_scheduletype_uri,
                        (SELECT m.Value FROM mapper m WHERE
                            m."Type" == "Supervisor Scheduler Permission" LIMIT 1) as mapper_supervisor_scheduler_permission,
                        CASE 
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m.Country = sgad._country_to_use_for_query AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1)) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m.Country = sgad._country_to_use_for_query AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) LIMIT 1)
                            WHEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1)) IS NOT NULL
                                THEN (SELECT m."value" FROM mapper m WHERE m."Type" == "Timeoff Template" AND m."Source" == 
                                        (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) LIMIT 1)
                            ELSE 
                                NULL
                        END as 'mapper_timeoff_template_master',
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timesheet Template' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'Others' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'R9,R4,RA,R8,TH,T4,TJ,TC,TI,T8,TK,TG,P0,P1,P5,P6,W0,W1,W5,W6' AND 
                            m.status == sgad.companycode
                            LIMIT 1
                        ) as mapper_timesheet_template,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timesheet Approval' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timesheet_approval_path,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timeoff Template' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timeoff_template,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timeoff Approval' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timeoff_approval,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timesheet Period' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timesheet_period,
                        (
                            CASE 
                                WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) IS NOT NULL
                                    THEN (SELECT m.Value FROM mapper m WHERE 
                                            m."Type" == 'WorkWeek' AND 
                                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                                            LIMIT 1
                                        )
                                ELSE 
                                    NULL
                            END
                        ) as mapper_work_week,
                        (
                            CASE 
                                WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) IS NOT NULL
                                    THEN (SELECT m.URI FROM mapper m WHERE 
                                            m."Type" == 'WorkWeek' AND 
                                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                                            LIMIT 1
                                        )
                                ELSE 
                                    NULL
                            END
                        ) as mapper_work_week_uri,
                        (SELECT GROUP_CONCAT(m."value", "|") FROM mapper m WHERE 
                            m."Type" == 'Activities' AND 
                            m."Source" == "GSAP" AND
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m.personnelsubarea == CASE 
                                WHEN sgad.areacode=="AU36" THEN "AU36"
                                ELSE "All Except AU36"
                            END
                        ) as mapper_activities,
                        (SELECT m.uri FROM mapper m WHERE 
                            m."Type" == 'Schedule Type' AND 
                            m.value == CASE 
                                WHEN ((LOWER(sgad.workshift) LIKE 'r%') OR (sgad.workshift == "Shift")) IS TRUE 
                                    THEN "Shift"
                                ELSE "Office Schedule"
                            END
                            LIMIT 1
                        ) as mapper_schedule_type_uri,
                        (SELECT m.value FROM mapper m WHERE 
                            m."Type" == "TimeZone" AND 
                            m.Country == sgad._country_to_use_for_query AND 
                            m."Source" == sgad._state_to_use_for_query
                            LIMIT 1
                        ) as mapper_timezone,
                        (SELECT m.uri FROM mapper m WHERE 
                            m."Type" == "TimeZone" AND 
                            m.Country == sgad._country_to_use_for_query AND 
                            m."Source" == sgad._state_to_use_for_query
                            LIMIT 1
                        ) as mapper_timezone_uri,
                        (SELECT m.value FROM mapper m WHERE 
                            m."Type" == "Holiday Calendar" AND 
                            m.Country == sgad._country_to_use_for_query AND 
                            m."Source" == sgad._state_to_use_for_query
                            LIMIT 1
                        ) as mapper_holiday_calendar,
                        CASE 
                            WHEN (SELECT m.value FROM mapper m WHERE m."type" == 'Country to enable' AND m.country = sgad._country_to_use_for_query LIMIT 1) IS NOT NULL
                                THEN
                                    CASE 
                                        WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) IS NOT NULL
                                            THEN
                                                "Enable"
                                        ELSE 
                                            NULL
                                    END
                            ELSE 
                                NULL
                        END as mapper_allowed_country,
                        (SELECT m.value FROM mapper m WHERE
                            m."Type" == 'Timesheet Period Effective Date' AND
                            m.country == sgad._country_to_use_for_query
                        ) as mapper_timesheet_period_effective_date,
                        CASE 
                            WHEN (SELECT m."status" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) IS NOT NULL
                                THEN
                                    (SELECT m."status" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            ELSE 
                                "disabled"
                        END as mapper_profile_status,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) IS NOT NULL
                                THEN
                                    (SELECT m."value" FROM mapper m WHERE m."Type" = "Time Entry Approval Path" AND
                                        m."source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                                    )
                            ELSE 
                                NULL 
                        END as mapper_timeentry_approval_path,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Payrule' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'Others' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'R9,R4,RA,R8,TH,T4,TJ,TC,TI,T8,TK,TG,P0,P1,P5,P6,W0,W1,W5,W6' AND 
                            m.status == sgad.companycode
                            LIMIT 1
                        ) as mapper_payrule,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timesheet Template' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'AU36' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'Others'
                            LIMIT 1
                        ) as mapper_timesheet_template_au36_3124,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Payrule' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'AU36' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'Others'
                            LIMIT 1
                        ) as mapper_payrule_au36_3124,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timesheet Template' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'AU36' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'R9,R4,RA,R8,TH,T4,TJ,TC,TI,T8,TK,TG,P0,P1,P5,P6,W0,W1,W5,W6'
                            LIMIT 1
                        ) as mapper_timesheet_template_au36_3124_empsubgrp_has_r9,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Payrule' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'AU36' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'R9,R4,RA,R8,TH,T4,TJ,TC,TI,T8,TK,TG,P0,P1,P5,P6,W0,W1,W5,W6'
                            LIMIT 1
                        ) as mapper_payrule_au36_3124_empsubgrp_has_r9,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == "Timesheet Template" AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'Others' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'Others' AND
                            m.status == sgad.companycode
                            LIMIT 1
                        ) as mapper_timesheet_template_notau36_empsubgrp_notin_r9list,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Payrule' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) AND
                            m.URI == sgad.'industrialinstrumentclassification' AND
                            m.personnelsubarea == 'Others' AND
                            m.employeegroup == CASE 
                                WHEN LOWER(sgad.workshift) LIKE 'r%' THEN 'Shift Schedule' 
                                ELSE 'Office Schedule' 
                            END AND
                            m.employeesubgroup == 'Others' AND
                            m.status == sgad.companycode
                            LIMIT 1
                        ) as mapper_payrule_notau36_empsubgrp_notin_r9list,
                        (SELECT m.value FROM mapper m WHERE 
                            m."type" == "Timesheet Template" AND 
                            m."Source" == (SELECT m3."Source" FROM mapper m3 WHERE m3."Type" = "Company Code" AND m3.uri == sgad.companycode LIMIT 1) AND
                            m."country" == sgad._country_to_use_for_query AND 
                            m."uri" == CASE 
                                WHEN sgad.ausjc IS NOT NULL THEN sgad.ausjc 
                                ELSE sgad.industrialinstrumentclassification
                            END AND
                            m.personnelsubarea == sgad.paygroup AND
                            m.employeegroup == CASE 
                                WHEN (SELECT m2."value" FROM mapper m2 WHERE m2."type" == "Schedule Type" AND m2.country == sgad._country_to_use_for_query AND m2."source" == sgad.workshift) == "Office Schedule"
                                    THEN "Office Schedule"
                                ELSE 
                                    "Shift Schedule"
                            END AND
                            m.employeesubgroup = sgad.fulltimeparttime
                        ) as mapper_timesheet_period_ia1_compass_aus,
                        (SELECT m.Value FROM mapper m WHERE 
                            m."Type" == 'Timesheet Approval' AND 
                            LOWER(m.Country) == LOWER(sgad._country_to_use_for_query) AND 
                            m."Source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timesheet_approval_path_ia1_compass_aus,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) == 'C1'
                                THEN sgad.subareacode || ' | ' || sgad.empgroupcode || ' | ' || empsubgroupcode
                                
                            ELSE
                                CASE
                                    WHEN sgad.exempt == "Yes"
                                        THEN "Exempt – Salaried"
                                    ELSE
                                        "Non Exempt - Hourly"
                                END
                        END as mapper_employee_type_fullpath_ia1_compass_aus,
                        (SELECT m.value FROM mapper m WHERE 
                            m."type"=="Timeoff Template" AND 
                            m.country == sgad._country_to_use_for_query AND 
                            m."source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timeoff_template_ia1_compass_aus,
                        (SELECT m.value FROM mapper m WHERE 
                            m."type"=="Timeoff Approval" AND 
                            m.country == sgad._country_to_use_for_query AND 
                            m."source" == (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1)
                            LIMIT 1
                        ) as mapper_timeoff_approval_ia1_compass_aus,
                        CASE 
                            WHEN sgad.ausjc IS NOT NULL
                                THEN CASE 
                                    WHEN (SELECT m.value FROM mapper m WHERE
                                            m."type" == "Timesheet Period" AND
                                            m."source" == "COMPASS" AND
                                            m.country == sgad._country_to_use_for_query AND
                                            m.uri == sgad.ausjc AND
                                            m.personnelsubarea == sgad.paygroup
                                            ) IS NOT NULL
                                        THEN 
                                            (SELECT m.value FROM mapper m WHERE
                                            m."type" == "Timesheet Period" AND
                                            m."source" == "COMPASS" AND
                                            m.uri == sgad.ausjc AND
                                            m.personnelsubarea == sgad.paygroup LIMIT 1)
                                            
                                    ELSE
                                        (SELECT m.value FROM mapper m WHERE 
                                            m."type" == "Timesheet Period" AND 
                                            m."source" == "COMPASS" AND 
                                            m.country == sgad._country_to_use_for_query AND
                                            m."uri" == "Default" LIMIT 1) 
                                END
                                
                            ELSE 
                                (SELECT m.value FROM mapper m WHERE 
                                    m."type" == "Timesheet Period" AND 
                                    m."source" == "COMPASS" AND 
                                    m."uri" == "Default" LIMIT 1)
                        END as timesheet_period_ia1_compass_aus,
                        CASE 
                            WHEN (SELECT m."Source" FROM mapper m WHERE m."Type" = "Company Code" AND m.uri == sgad.companycode LIMIT 1) == "COMPASS"
                                THEN (SELECT GROUP_CONCAT(m.value, '|') FROM mapper m WHERE
                                        m."type" == "Activities" AND
                                        m.country == sgad._country_to_use_for_query AND
                                        m."source" == "COMPASS" AND
                                        m.personnelsubarea == CASE
                                            WHEN (SELECT m2.value FROM mapper m2 WHERE 
                                                    m2."type" == "Schedule Type" AND 
                                                    m2.country == sgad._country_to_use_for_query AND
                                                    m2."Source" == sgad.workshift LIMIT 1) == "Office Schedule"
                                                THEN "Office Schedule"
                                                
                                            ELSE
                                                "Shift Schedule"
                                        END LIMIT 1
                                    )
                            ELSE
                                NULL
                        END as mapper_activity_list_ia1_compass_aus,
                        (SELECT m.value FROM mapper m WHERE 
                            m."type" == "TimeZone" AND
                            m.country == sgad._country_to_use_for_query AND
                            m."source" == sgad._state_to_use_for_query) as mapper_timezone_ia1_compass_aus,
                        (SELECT m.uri FROM mapper m WHERE 
                            m."type" == "TimeZone" AND
                            m.country == sgad._country_to_use_for_query AND
                            m."source" == sgad._state_to_use_for_query) as mapper_timezone_uri_ia1_compass_aus,
                        (SELECT m.value FROM mapper m WHERE 
                            m."type" == "Holiday Calendar" AND
                            m.country == sgad._country_to_use_for_query AND
                            m."source" == sgad._state_to_use_for_query LIMIT 1) as mapper_holiday_calendar_ia1_compass_aus,
                        CASE 
                            WHEN (SELECT m.value FROM mapper m WHERE 
                                    m."type" == "Country to enable" AND
                                    m."country" == sgad._country_to_use_for_query) IS NOT NULL
                                THEN CASE 
                                    WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) IS NOT NULL 
                                        THEN "Enable"
                                    ELSE 
                                        NULL
                                END
                            ELSE 
                                NULL 
                        END as mapper_allowed_country_ia1_compass_aus ,
                        (SELECT m.value FROM mapper m WHERE
                            m."type" == "Timesheet Period Effective Date" AND
                            m.country == sgad._country_to_use_for_query) as mapper_timesheet_period_effective_date_ia1_compass_aus,
                        CASE 
                            WHEN sgad.ausjc IS NOT NULL 
                                THEN 
                                    (SELECT m.value FROM mapper m WHERE 
                                    m."type" == "Payrule" AND 
                                    m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) AND
                                    m.country == sgad._country_to_use_for_query AND 
                                    m.uri == sgad.ausjc AND 
                                    m.personnelsubarea == sgad.paygroup AND
                                    m.employeesubgroup == sgad.fulltimeparttime AND
                                    m.employeegroup ==  CASE 
                                        WHEN (SELECT m3.value FROM mapper m3 WHERE 
                                            m3."type" == "Schedule Type" AND 
                                            m3."country" == sgad._country_to_use_for_query AND 
                                            m3."source" == sgad.workshift) == "Office Schedule"
                                            THEN "Office Schedule"
                                        ELSE
                                            "Shift Schedule"
                                    END LIMIT 1)
                                ELSE 
                                    (SELECT m.value FROM mapper m WHERE 
                                    m."type" == "Payrule" AND 
                                    m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) AND
                                    m.country == sgad._country_to_use_for_query AND 
                                    m.uri == sgad.industrialinstrumentclassification AND 
                                    m.employeegroup ==  CASE 
                                        WHEN (SELECT m3.value FROM mapper m3 WHERE 
                                            m3."type" == "Schedule Type" AND 
                                            m3."country" == sgad._country_to_use_for_query AND 
                                            m3."source" == sgad.workshift) == "Office Schedule"
                                            THEN "Office Schedule"
                                        ELSE
                                            "Shift Schedule"
                                    END LIMIT 1)
                        END AS mapper_payrule_ia1_compass_aus,
                        CASE
                            WHEN (SELECT m.value FROM mapper m WHERE
                                    m."type" == "Schedule Type" AND
                                    m."country" == sgad._country_to_use_for_query AND
                                    m."source" == sgad.workshift) == "Office Schedule"
                                THEN sgad.workshift
                            ELSE 
                                "Shift"
                        END AS mapper_schedule_name_ia1_compass_aus,
                        CASE
                            WHEN (SELECT m.value FROM mapper m WHERE
                                m."type" == "Schedule Type" AND
                                m.country == sgad._country_to_use_for_query AND
                                m."Source" == sgad.workshift LIMIT 1) == "Office Schedule" 
                                THEN sgad.workshift
                            ELSE
                                "Shift"
                        END AS mapper_schedule_name_uri_ia1_compass_aus,
                        CASE 
                            WHEN sgad.ausjc IS NOT NULL
                                THEN (SELECT m.value FROM mapper m WHERE 
                                    m."type" == "Timesheet Template" AND 
                                    m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) AND
                                    m.country == sgad._country_to_use_for_query AND 
                                    m.uri == sgad.ausjc AND 
                                    m.personnelsubarea == sgad.paygroup AND
                                    m.employeesubgroup == sgad.fulltimeparttime AND
                                    m.employeegroup ==  CASE 
                                        WHEN (SELECT m3.value FROM mapper m3 WHERE 
                                            m3."type" == "Schedule Type" AND 
                                            m3."country" == sgad._country_to_use_for_query AND 
                                            m3."source" == sgad.workshift) == "Office Schedule"
                                            THEN "Office Schedule"
                                        ELSE
                                            "Shift Schedule"
                                    END LIMIT 1)
                            ELSE (SELECT m.value FROM mapper m WHERE 
                                    m."type" == "Timesheet Template" AND 
                                    m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) AND
                                    m.country == sgad._country_to_use_for_query AND 
                                    m.uri == sgad.industrialinstrumentclassification AND 
                                    m.employeegroup ==  CASE 
                                        WHEN (SELECT m3.value FROM mapper m3 WHERE 
                                            m3."type" == "Schedule Type" AND 
                                            m3."country" == sgad._country_to_use_for_query AND 
                                            m3."source" == sgad.workshift) == "Office Schedule"
                                            THEN "Office Schedule"
                                        ELSE
                                            "Shift Schedule"
                                    END LIMIT 1)
                        END as mapper_timesheet_template_aus_compass,
                        (SELECT m.uri FROM mapper m WHERE 
                            m."type" == "Schedule Type" AND
                            m."country" == sgad._country_to_use_for_query AND 
                            m."source" == sgad.workshift) as mapper_shift_type_uri_aus_comapss,
                        CASE 
                            WHEN sgad.ausjc IS NOT NULL 
                                THEN CASE
                                        WHEN (SELECT m.value FROM mapper m WHERE
                                            m."type" == "Timesheet Period" AND 
                                            m."Source" == "COMPASS" AND 
                                            m."country" == sgad._country_to_use_for_query AND 
                                            m.uri == sgad.ausjc AND 
                                            m.personnelsubarea == sgad.paygroup
                                        ) IS NOT NULL
                                            THEN (SELECT m.value FROM mapper m WHERE
                                                m."type" == "Timesheet Period" AND 
                                                m."Source" == "COMPASS" AND 
                                                m."country" == sgad._country_to_use_for_query AND 
                                                m.uri == sgad.ausjc AND 
                                                m.personnelsubarea == sgad.paygroup
                                            )
                                        ELSE
                                            (SELECT m.value FROM mapper m WHERE
                                                m."type" == "Timesheet Period" AND 
                                                m."Source" == "COMPASS" AND 
                                                m."country" == sgad._country_to_use_for_query AND 
                                                m.uri == "Default"
                                        )
                                END
                            ELSE (SELECT m.value FROM mapper m WHERE
                                    m."type" == "Timesheet Period" AND 
                                    m."Source" == "COMPASS" AND 
                                    m.uri == "Default" 
                                )
                        END AS mapper_timesheet_period_aus_compass,
                        CASE 
                            WHEN (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) == "COMPASS"
                                THEN (SELECT GROUP_CONCAT(m.value, '|') FROM mapper m WHERE 
                                    m."type" == "Activities" AND 
                                    m.country == sgad._country_to_use_for_query AND 
                                    m."Source" == "COMPASS" AND 
                                    m.personnelsubarea == CASE
                                        WHEN (SELECT m2.value FROM mapper m2 WHERE 
                                                m2."type" == "Schedule Type" AND
                                                m2.country == sgad._country_to_use_for_query AND
                                                m2."Source"== sgad.workshift) == "Office Schedule"
                                            THEN 
                                                "Office Schedule"
                                        ELSE "Shift Schedule"
                                    END LIMIT 1)
                            ELSE 
                                NULL 
                        END AS mapper_activity_list_aus_compass,
                        CASE
                            WHEN (SELECT m.value FROM mapper m WHERE
                                m."type" == "Schedule Type" AND
                                m.country == sgad._country_to_use_for_query AND
                                m."Source" == sgad.workshift LIMIT 1) == "Office Schedule" 
                                THEN sgad.workshift
                            ELSE
                                "Shift"
                        END	AS mapper_schedule_name_aus_compass,
                        CASE 
                            WHEN sgad.ausjc
                                THEN (SELECT m.value FROM mapper m WHERE
                                    m."type" == "Payrule" AND
                                    m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) AND
                                    m."country" == sgad._country_to_use_for_query AND 
                                    m."uri" == sgad.ausjc AND 
                                    m."personnelsubarea" == sgad.paygroup AND 
                                    m."employeegroup" == CASE 
                                            WHEN (SELECT m3.value FROM mapper m3 WHERE 
                                                m3."type" == "Schedule Type" AND 
                                                m3."country" == sgad._country_to_use_for_query AND 
                                                m3."source" == sgad.workshift) == "Office Schedule"
                                                THEN "Office Schedule"
                                            ELSE
                                                "Shift Schedule"
                                        END AND 
                                    m.employeesubgroup == sgad.fulltimeparttime
                                )
                            ELSE
                                (SELECT m.value FROM mapper m WHERE
                                    m."type" == "Payrule" AND
                                    m."source" == (SELECT m2."Source" FROM mapper m2 WHERE m2."Type" = "Company Code" AND m2.uri == sgad.companycode LIMIT 1) AND
                                    m."country" == sgad._country_to_use_for_query AND 
                                    m."uri" == sgad.ausjc AND 
                                    m."personnelsubarea" == sgad.industrialinstrumentclassification AND 
                                    m."employeegroup" == CASE 
                                            WHEN (SELECT m3.value FROM mapper m3 WHERE 
                                                m3."type" == "Schedule Type" AND 
                                                m3."country" == sgad._country_to_use_for_query AND 
                                                m3."source" == sgad.workshift) == "Office Schedule"
                                                THEN "Office Schedule"
                                            ELSE
                                                "Shift Schedule"
                                        END AND 
                                    m.employeesubgroup == sgad.fulltimeparttime
                                )
                        END AS mapper_payrule_aus_compass,
                        CASE
                            WHEN sgad.terminationreason
                                THEN (SELECT m.value FROM mapper m WHERE
                                        m."type"=="Termination Reason" AND
                                        m."source" == sgad.terminationreason AND
                                        m."uri" == sgad._state_to_use_for_query LIMIT 1)
                            ELSE
                                NULL
                        END as mapper_termination_reason_code
                        FROM splitter_gsap_all_data sgad"""
            
        )

        start_task, end_task = get_all_required_fields("gsap_get_all_records", config)

        @lru_cache(maxsize=8)
        def items():
            return rail.load_all_records(rail.result("get_gsap_data"))

        trigger_process_user = rail.trigger_parallel_dagrun(
            task_id = "trigger_process_user",
            items=items,
            trigger_dag_id=config.workday_user_import_process_users_child_dag_dag_ids_per_erp['gsap'],
            parallel_count=config.process_users_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf = lambda item, dag_run : get_gsap_user_process_conf(item, dag_run, config)
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


        get_gsap_data >> start_task

        end_task >> trigger_process_user >> get_all_run_ids >> gather_all_logs


    return dag

rail.for_each_instance(create_dag)
