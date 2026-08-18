# pylint:disable=multiple-statements
region = 'us-east-1'
environment = 'pre-production'
instance = "aurasb"
company_key = 'aurasb'
replicon_conn_id = f'polaris_{company_key}'
tenant_email = "AvnishKhurana@deltek.com"
internal_email = "MPTeamReplicon@deltek.com"
time_zone = 'US/Eastern'
execution_timeout_days = 14
child_dag_max_active_runs = 2
can_run_batch_task_var_name = f'Cospoint_timeoff_import_can_run_batch_task_{instance}'
costpoint_date_format = '%Y-%m-%dT%H:%M:%S'
polaris_date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
master_dag_interval = 3600
odbc_conn_id = f'Polaris_deltek_odbc_{company_key}'
costpoint_to_date_format = '%Y-%m-%d %H:%M:%S'
replicon_timeoff_date_format = '%m/%d/%Y'
create_holiday_as_timeoff = False
last_run_date_var_name = f'{company_key}_deltek_costpoint_timeoff_sync_last_run_date'
cp_timezone = "America/New_York"
number_of_timeoff_data_periods = 1
number_of_days_future = 50
number_of_days_past = 50
cp_holiday_name = "HOLIDAY"
cp_timeoff_name = "Time Off"
master_delete_dag_interval = 172800
cp_timeoff_file_name = "costpoint-aura-timeoff.csv"
s3_location_for_cp_timeoff = "cp-timeoff-details"
s3_bucket_name = "airflow-systemtest"
aws_conn_id = 'replicon_bucket_conn'
chunk_size = 10
isFromSql = False
sql_query = None
sql_delete_query = None
deltek_cospoint_sql_conn_id = ""
odbc_query = """SELECT t.EMPL_ID AS USER_ID
            , CASE WHEN lt.HOLIDAY_FL = 'Y' THEN 'HOLIDAY'    -- Linking to LEAVE_TYPE table to determine
                WHEN lt.VACATION_FL = 'Y' THEN 'LEAVE' ELSE '?' END AS TIME_OFF_TYPE
            , lt.LEAVE_TYPE_CD AS LEAVE_TYPE_CD
            , tc.HRS_DT AS TIME_OFF_STARTDATE, tc.HRS_DT AS TIME_OFF_ENDDATE
            , SUM(tc.ENTERED_HRS) AS TIME_OFF_HOURS           -- Summing hours by day
            , t.TIME_STAMP AS LAST_MODIFIED
            , tc.ROWVERSION AS ROWVERSION                     -- 0 for new records, 1 for modifications
            , tl.S_APPROVE_STATUS_CD AS TIME_OFF_STATUS       -- Shows if approved
            FROM TS t                                   -- 3 tables on timesheet - TS, TS_LINE, TS_CELL
            INNER JOIN TS_LINE tl                       -- TS_LINE is where pay type info is
            ON tl.EMPL_ID = t.EMPL_ID
            AND tl.TS_SCHEDULE_CD = t.TS_SCHEDULE_CD
            AND tl.YEAR_NO_CD = t.YEAR_NO_CD
            AND tl.PERIOD_NO_CD = t.PERIOD_NO_CD
            INNER JOIN TS_CELL tc                       -- TS_CELL is where entered hours are
            ON tc.EMPL_ID = tl.EMPL_ID
            AND tc.TS_SCHEDULE_CD = tl.TS_SCHEDULE_CD
            AND tc.YEAR_NO_CD = tl.YEAR_NO_CD
            AND tc.PERIOD_NO_CD = tl.PERIOD_NO_CD
            AND tc.LINE_NO = tl.LINE_NO
            INNER JOIN LEAVE_TYPE lt                    -- Leave types
            ON lt.LEAVE_TYPE_CD = tl.UDT10_ID
            WHERE t.TIME_STAMP > ?                 -- Records since last modified date
            GROUP BY t.EMPL_ID, t.TIME_STAMP, tc.HRS_DT, tl.UDT10_ID, tl.S_APPROVE_STATUS_CD
            , lt.HOLIDAY_FL, lt.VACATION_FL, lt.LEAVE_TYPE_CD, tc.ROWVERSION
            UNION ALL
            -- Holiday Time Scheduled for Individual Employees - expended for 'ALL' employees
            SELECT CASE WHEN e.EMPL_ID IS NULL THEN wsd.EMPL_ID ELSE e.EMPL_ID END AS USER_ID
            , 'HOLIDAY' AS TIME_OFF_TYPE
            , ' ' AS LEAVE_TYPE_CD
            , wsd.SCHEDULE_DT AS TIME_OFF_STARTDATE, wsd.SCHEDULE_DT AS TIME_OFF_ENDDATE
            , wsd.STANDARD_HRS AS TIME_OFF_HOURS
            , wsd.TIME_STAMP AS LAST_MODIFIED
            , wsd.ROWVERSION AS ROWVERSION        -- 0 for new records, 1 for modifications
            , CASE WHEN wsd.PENDING_APPROVAL_FL = 'Y' THEN 'PENDING' ELSE 'APPROVED' END AS TIME_OFF_STATUS
            FROM WORK_SCHEDULE_DATE wsd
            LEFT OUTER JOIN EMPL e
            ON e.ACTIVE_FL = 'Y'                  -- Employee is active
            AND wsd.EMPL_ID = 'ALL'               -- Expand selection for 'ALL'
            AND e.HIRE_DT <= wsd.SCHEDULE_DT      -- After Hire Date
            AND (e.TERMINATE_DT IS NULL OR e.TERMINATE_DT >= wsd.SCHEDULE_DT) -- Before Term date
            LEFT OUTER JOIN (                       -- Added to find max timesheet date for employee
                SELECT EMPL_ID, MAX(HRS_DT) AS MAX_TS_DT
                FROM TS_CELL
                GROUP BY EMPL_ID) maxts
            ON maxts.EMPL_ID = e.EMPL_ID
            WHERE wsd.HOLIDAY_FL = 'Y'              -- Holiday Flag
            AND wsd.NON_WORKDAY_FL != 'Y'     -- Ignore non-work days
            AND wsd.TIME_STAMP > ?     -- All changes after a particular date
            AND wsd.SCHEDULE_DT > maxts.MAX_TS_DT -- No future records after max TS date
            UNION ALL
            -- Vacation Time Scheduled for Individual Employees
            SELECT wsd.EMPL_ID AS USER_ID
            , 'LEAVE' AS TIME_OFF_TYPE
            , ' ' AS LEAVE_TYPE_CD
            , wsd.SCHEDULE_DT AS TIME_OFF_STARTDATE, wsd.SCHEDULE_DT AS TIME_OFF_ENDDATE
            , wsd.LEAVE_HRS AS TIME_OFF_HOURS
            , wsd.TIME_STAMP AS LAST_MODIFIED
            , wsd.ROWVERSION AS ROWVERSION        -- 0 for new records, 1 for modifications
            , CASE WHEN wsd.PENDING_APPROVAL_FL = 'Y' THEN 'PENDING' ELSE 'APPROVED' END AS TIME_OFF_STATUS
            FROM WORK_SCHEDULE_DATE wsd
            WHERE wsd.VACATION_FL = 'Y'             -- Vacation Flag
            AND wsd.NON_WORKDAY_FL != 'Y'       -- Ignore non-work days
            AND wsd.TIME_STAMP > ?    -- All changes after a particular date
            ORDER BY USER_ID, TIME_OFF_STARTDATE"""
delete_odbc_query = """SELECT t.EMPL_ID AS USER_ID
            , CASE WHEN lt.HOLIDAY_FL = 'Y' THEN 'HOLIDAY'    -- Linking to LEAVE_TYPE table to determine
                WHEN lt.VACATION_FL = 'Y' THEN 'LEAVE' ELSE '?' END AS TIME_OFF_TYPE
            , lt.LEAVE_TYPE_CD AS LEAVE_TYPE_CD
            , tc.HRS_DT AS TIME_OFF_STARTDATE, tc.HRS_DT AS TIME_OFF_ENDDATE
            , SUM(tc.ENTERED_HRS) AS TIME_OFF_HOURS           -- Summing hours by day
            , t.TIME_STAMP AS LAST_MODIFIED
            , tc.ROWVERSION AS ROWVERSION                     -- 0 for new records, 1 for modifications
            , tl.S_APPROVE_STATUS_CD AS TIME_OFF_STATUS       -- Shows if approved
            FROM TS t                                   -- 3 tables on timesheet - TS, TS_LINE, TS_CELL
            INNER JOIN TS_LINE tl                       -- TS_LINE is where pay type info is
            ON tl.EMPL_ID = t.EMPL_ID
            AND tl.TS_SCHEDULE_CD = t.TS_SCHEDULE_CD
            AND tl.YEAR_NO_CD = t.YEAR_NO_CD
            AND tl.PERIOD_NO_CD = t.PERIOD_NO_CD
            INNER JOIN TS_CELL tc                       -- TS_CELL is where entered hours are
            ON tc.EMPL_ID = tl.EMPL_ID
            AND tc.TS_SCHEDULE_CD = tl.TS_SCHEDULE_CD
            AND tc.YEAR_NO_CD = tl.YEAR_NO_CD
            AND tc.PERIOD_NO_CD = tl.PERIOD_NO_CD
            AND tc.LINE_NO = tl.LINE_NO
            INNER JOIN LEAVE_TYPE lt                    -- Leave types
            ON lt.LEAVE_TYPE_CD = tl.UDT10_ID
            WHERE tc.HRS_DT BETWEEN ? AND ?
            --WHERE t.TIME_STAMP > ?                 -- Records since last modified date
            GROUP BY t.EMPL_ID, t.TIME_STAMP, tc.HRS_DT, tl.UDT10_ID, tl.S_APPROVE_STATUS_CD
            , lt.HOLIDAY_FL, lt.VACATION_FL, lt.LEAVE_TYPE_CD, tc.ROWVERSION
            UNION ALL
            -- Holiday Time Scheduled for Individual Employees - expended for 'ALL' employees
            SELECT CASE WHEN e.EMPL_ID IS NULL THEN wsd.EMPL_ID ELSE e.EMPL_ID END AS USER_ID
            , 'HOLIDAY' AS TIME_OFF_TYPE
            , ' ' AS LEAVE_TYPE_CD
            , wsd.SCHEDULE_DT AS TIME_OFF_STARTDATE, wsd.SCHEDULE_DT AS TIME_OFF_ENDDATE
            , wsd.STANDARD_HRS AS TIME_OFF_HOURS
            , wsd.TIME_STAMP AS LAST_MODIFIED
            , wsd.ROWVERSION AS ROWVERSION        -- 0 for new records, 1 for modifications
            , CASE WHEN wsd.PENDING_APPROVAL_FL = 'Y' THEN 'PENDING' ELSE 'APPROVED' END AS TIME_OFF_STATUS
            FROM WORK_SCHEDULE_DATE wsd
            LEFT OUTER JOIN EMPL e
            ON e.ACTIVE_FL = 'Y'                  -- Employee is active
            AND wsd.EMPL_ID = 'ALL'               -- Expand selection for 'ALL'
            AND e.HIRE_DT <= wsd.SCHEDULE_DT      -- After Hire Date
            AND (e.TERMINATE_DT IS NULL OR e.TERMINATE_DT >= wsd.SCHEDULE_DT) -- Before Term date
            LEFT OUTER JOIN (                       -- Added to find max timesheet date for employee
                SELECT EMPL_ID, MAX(HRS_DT) AS MAX_TS_DT
                FROM TS_CELL
                GROUP BY EMPL_ID) maxts
            ON maxts.EMPL_ID = e.EMPL_ID
            WHERE wsd.HOLIDAY_FL = 'Y'              -- Holiday Flag
            AND wsd.NON_WORKDAY_FL != 'Y'     -- Ignore non-work days
            AND wsd.SCHEDULE_DT BETWEEN ? AND ?
            --AND wsd.TIME_STAMP > ?     -- All changes after a particular date
            AND wsd.SCHEDULE_DT > maxts.MAX_TS_DT -- No future records after max TS date
            UNION ALL
            -- Vacation Time Scheduled for Individual Employees
            SELECT wsd.EMPL_ID AS USER_ID
            , 'LEAVE' AS TIME_OFF_TYPE
            , ' ' AS LEAVE_TYPE_CD
            , wsd.SCHEDULE_DT AS TIME_OFF_STARTDATE, wsd.SCHEDULE_DT AS TIME_OFF_ENDDATE
            , wsd.LEAVE_HRS AS TIME_OFF_HOURS
            , wsd.TIME_STAMP AS LAST_MODIFIED
            , wsd.ROWVERSION AS ROWVERSION        -- 0 for new records, 1 for modifications
            , CASE WHEN wsd.PENDING_APPROVAL_FL = 'Y' THEN 'PENDING' ELSE 'APPROVED' END AS TIME_OFF_STATUS
            FROM WORK_SCHEDULE_DATE wsd
            WHERE wsd.VACATION_FL = 'Y'             -- Vacation Flag
            AND wsd.NON_WORKDAY_FL != 'Y'       -- Ignore non-work days
            AND wsd.SCHEDULE_DT BETWEEN ? AND ?
            --AND wsd.TIME_STAMP > ?    -- All changes after a particular date
            ORDER BY USER_ID, TIME_OFF_STARTDATE"""
