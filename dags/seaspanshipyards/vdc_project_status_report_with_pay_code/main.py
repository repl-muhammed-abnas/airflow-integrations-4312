from hashlib import md5
from pendulum import datetime
from seaspanshipyards.vdc_project_status_report_with_pay_code import custom_methods, request_payload
import rail
# pylint:disable=too-many-statements


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"seaspanshipyards_vdc_project_status_report_with_pay_code_master_dag_{config.instance}",
        description="seaspanshipyards vdc_project_status_report_with _pay_code",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 8, 29, tz="PST8PDT"),
        max_active_runs=config.max_active_runs,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.webhook_shared_secret
        )
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        get_requestor_details = rail.RepliconServiceOperator(
            task_id="get_requestor_details",
            endpoint="/services/UserService1.svc/GetUserDetails",
            data=request_payload.get_user_data_request,
            data_handler=lambda response: response["emailAddress"]
        )

        if_email_present_for_requestor = rail.IfOperator(
            task_id="if_email_present_for_requestor",
            test='{{result("get_requestor_details")| is_truthy}}',
            yes_task="if_daterange_value_present",
            no_task="log_to_sumo"
        )

        if_daterange_value_present = rail.IfOperator(
            task_id="if_daterange_value_present",
            test=lambda dag_run: bool(
                "daterange" in dag_run.conf['webhook']['data'] and dag_run.conf['webhook']['data']["daterange"]),
            yes_task="get_assigned_user_permissions",
            no_task="send_invalid_daterange_mail"
        )

        get_assigned_user_permissions = rail.RepliconServiceOperator(
            task_id="get_assigned_user_permissions",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=request_payload.get_user_data_request,
            data_handler=lambda response: list(map(lambda i: {
                "name": i["permissionSet"]["name"],
                "policyUri": i["policyUri"]
            }, response)),
        )

        send_invalid_daterange_mail = rail.EmailOperator(
            task_id="send_invalid_daterange_mail",
            to='{{result("get_requestor_details")}}',
            subject='{{get_company_key()}}' + "| Custom Paycode Report - Skipped" +
            '{{ current_time_in_specified_tz(fmt="%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/incorrect_daterange_mail.html"
        )

        if_valid_permissions_for_requestor = rail.IfOperator(
            task_id="if_valid_permissions_for_requestor",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_assigned_user_permissions"),
                "policyUri", "urn:replicon:policy:payroll-management", "name") and
                rail.find_first_by_attr_and_get_attr(
                rail.result("get_assigned_user_permissions"),
                "policyUri", "urn:replicon:policy:supervision", "name")),
            yes_task="get_project_hours_report_details",
            no_task="send_invalid_permissions_mail"
        )

        get_project_hours_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_project_hours_report_details",
            report_name=config.project_export_report_name,
        )

        send_invalid_permissions_mail = rail.EmailOperator(
            task_id="send_invalid_permissions_mail",
            to='{{result("get_requestor_details")}}',
            subject='{{get_company_key()}}' + "| Custom Paycode Report - Skipped" +
            '{{ current_time_in_specified_tz(fmt="%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/insufficient_permission_mail.html"
        )
        report_entry, report_exit = rail.run_report(
            group_id="project_hour_report_run",
            report_params=custom_methods.get_report_params
        )

        if_report_generation_successful = rail.IfOperator(
            task_id="if_report_generation_successful",
            test='{{result("project_hour_report_run.get_report_result").reportGenerationResults[0].error|is_truthy}}',
            no_task="if_report_has_data",
            yes_task="fail_dagrun"
        )

        if_report_has_data = rail.IfOperator(
            task_id="if_report_has_data",
            test='{{result("project_hour_report_run.get_report_result","has_data")}}',
            yes_task="if_payload_has_appropriate_columns",
            no_task="send_no_data_mail"
        )

        if_payload_has_appropriate_columns = rail.IfOperator(
            task_id="if_payload_has_appropriate_columns",
            # pylint: disable=consider-using-f-string line-too-long
            test="{{result('project_hour_report_run.get_report_result').reportGenerationResults[0].payload | starts_with('%s')}}" % config.expected_report_columns,
            yes_task="load_project_data_csv",
            no_task="fail_dagrun"
        )

        load_project_data_csv = rail.LoadCSVFileOperator(
            task_id="load_project_data_csv",
            document='{{result("project_hour_report_run.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_project_data_collection = rail.CreateCollectionOperator(
            task_id="create_project_data_collection",
            source='{{result("load_project_data_csv")}}',
            name="projectdata",
            columns={
                'Entry Date': 'entrydate',
                'User Name': 'username',
                'VDC Override Shift': 'VDCOverrideShift',
                'Override Shift': 'OverrideShift',
                'User Supervisor Name (Current)': 'supervisorname',
                'Trade Name': 'tradename',
                'Task Name (Full Path)': 'taskname',
                'Project Hrs': 'projecthours',
                'Employee ID': 'employeeid',
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Task Code': 'taskcode',
                'Approval Status': 'approvalstatus',
                'supervisoruri': 'supervisoruri',
                'projecturi': 'projecturi'
            }
        )

        query_valid_project_data = rail.QueryCollectionOperator(
            task_id="query_valid_project_data",
            query="""SELECT * FROM projectdata WHERE projecthours > "0.00" AND NULLIF(projectname,"") IS NOT NULL"""
        )

        write_valid_project_data_to_csv = rail.WriteCSVFileOperator(
            task_id="write_valid_project_data_to_csv",
            source='{{result("query_valid_project_data")}}',
            header=["entrydate", "username", "vdcoverrideshifttimesheetdailyfield", "overrideshifttimesheetdailyfield",
                    "usersupervisornamecurrent", "tradegroupcurrentuserudf", "tasknamefullpath", "projecthours",
                    "eeid", "projectname", "projectcode", "taskcode", "approvalstatus", "supervisoruri",
                    "supervisorcheck", "projecturi", "projectcheck", "mergeid", "uniqueid"],
            row=custom_methods.get_validated_row
        )

        create_validateddata_collection = rail.CreateCollectionOperator(
            task_id="create_validateddata_collection",
            name="validateddata",
            source='{{result("write_valid_project_data_to_csv")}}'
        )

        query_validated_project_data_with_uniqueid = rail.QueryCollectionOperator(
            task_id="query_validated_project_data_with_uniqueid",
            query="""SELECT * FROM projectdata WHERE projecthours > "0.00" AND NULLIF(projectname,"") IS NOT NULL"""
        )

        if_valid_project_data = rail.IfOperator(
            task_id="if_valid_project_data",
            test='{{result("query_validated_project_data_with_uniqueid", "length") > 0}}',
            no_task="send_no_data_mail",
            yes_task="query_valid_vdc_vsy_shift_data"
        )

        query_valid_vdc_vsy_shift_data = rail.QueryCollectionOperator(
            task_id="query_valid_vdc_vsy_shift_data",
            query="""SELECT DISTINCT entrydate, username,VDCOverrideShift, OverrideShift, employeeid
                    FROM projectdata WHERE NULLIF(VDCOverrideShift,"") IS NOT NULL OR
                    NULLIF(OverrideShift,"") IS NOT NULL """
        )

        write_vdc_vsy_shift_data_csv = rail.WriteCSVFileOperator(
            task_id="write_vdc_vsy_shift_data_csv",
            source='{{result("query_valid_vdc_vsy_shift_data")}}',
            header=["entrydate", "username", "vdcoverrideshifttimesheetdailyfield",
                    "overrideshifttimesheetdailyfield", "eeid", "uniqueid"],
            row=lambda item: [
                item["entrydate"], item["username"], item["VDCOverrideShift"], item["OverrideShift"],
                item["employeeid"],
                md5(("_".join([item["entrydate"], item["username"],
                    item["employeeid"]])).encode()).hexdigest()
            ]
        )

        create_vdc_vsy_shift_data_collection = rail.CreateCollectionOperator(
            task_id="create_vdc_vsy_shift_data_collection",
            source='{{result("write_vdc_vsy_shift_data_csv")}}',
            columns={
                    "entrydate": "entrydate1",
                    "username": "username1",
                    "vdcoverrideshifttimesheetdailyfield": "VDCOverrideShift1",
                    "overrideshifttimesheetdailyfield": "OverrideShift1",
                    "eeid": "employeeid1",
                    "uniqueid": "uniqueid1"
            },
            name="validatedvdcshiftdata"
        )

        query_merge_report_data_with_vdc_shift = rail.QueryCollectionOperator(
            task_id="query_merge_report_data_with_vdc_shift",
            query="""SELECT entrydate,username,VDCOverrideShift1,OverrideShift1,
                    usersupervisornamecurrent,tradegroupcurrentuserudf,
                    tasknamefullpath,projecthours,eeid,projectname,
                    projectcode,taskcode,approvalstatus,supervisoruri,supervisorcheck,
                    projecturi, projectcheck, mergeid, uniqueid
                    FROM validatedvdcshiftdata, validateddata WHERE uniqueid = uniqueid1""",
        )

        query_merge_report_data_without_vdc_shift = rail.QueryCollectionOperator(
            task_id="query_merge_report_data_without_vdc_shift",
            query="""SELECT * FROM validateddata WHERE uniqueid NOT IN
                    (SELECT DISTINCT uniqueid1 FROM validatedvdcshiftdata)"""
        )

        query_create_final_validateddata = rail.QueryCollectionOperator(
            task_id="query_create_final_validateddata",
            name="finalvalidateddata",
            query="""SELECT * FROM query_merge_report_data_with_vdc_shift UNION
                    SELECT * FROM  query_merge_report_data_without_vdc_shift"""
        )

        query_supervisor_project_in_report_data = rail.QueryCollectionOperator(
            task_id="query_supervisor_project_in_report_data",
            query="""SELECT * FROM finalvalidateddata WHERE supervisorcheck="Yes" AND projectcheck="Yes" """
        )

        get_all_paycode_scripts = rail.RepliconServiceOperator(
            task_id="get_all_paycode_scripts",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts"
        )

        create_paycode_download_batch = rail.RepliconServiceOperator(
            task_id="create_paycode_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.payroll_batch_request
        )

        batch_uri = '{{result("create_paycode_download_batch")}}'
        run_paycode_download_batch, wait_for_paycode_batch = rail.batch_execution(
            "payrule_run_batch",
            create_paycode_download_batch.task_id
        )

        get_payroll_run_batch_result = rail.RepliconServiceOperator(
            task_id="get_payroll_run_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={"payrollDownloadBatchUri": batch_uri},
            data_handler=lambda response: response["downloadUrl"]
        )

        download_paycode_batch_file = rail.HTTPDownloadFileOperator(
            task_id="download_paycode_batch_file",
            url='{{result("get_payroll_run_batch_result")}}'
        )

        load_paycode_file_to_csv = rail.LoadCSVFileOperator(
            task_id="load_paycode_file_to_csv",
            document="{{result('download_paycode_batch_file')}}"
        )
        create_paycode_collection = rail.CreateCollectionOperator(
            task_id="create_paycode_collection",
            source='{{result("load_paycode_file_to_csv")}}',
            columns={
                    "Entry Date": "entrydate", "User": "username", "Trade Name": "tradename", "Task Name": "taskname",
                    "Employee ID": "employeeid", "Project Name": "projectname", "Project Code": "projectcode",
                    "Task Code": "taskcode", "Pay Code Name": "paycodename", "Pay Code Code": "paycodecode", "Pay COde Hours": "paycodehrs"
            },
            name="paycodedata"
        )

        query_to_valid_user_paycode_data = rail.QueryCollectionOperator(
            task_id="query_to_valid_user_paycode_data",
            query="""SELECT * FROM paycodedata WHERE username IN (SELECT DISTINCT username FROM query_supervisor_project_in_report_data)""",
        )

        write_project_paycode_data_mergeid_csv = rail.WriteCSVFileOperator(
            task_id="write_project_paycode_data_mergeid_csv",
            source='{{result("query_to_valid_user_paycode_data")}}',
            header=["entrydate", "username", "tradename", "taskname", "employeeid", "projectname",
                    "projectcode", "taskcode", "paycodename", "paycodecode", "paycodehrs", "mergeid"],
            row=custom_methods.get_pay_code_row
        )

        create_collection_project_paycode_data = rail.CreateCollectionOperator(
            task_id="create_collection_project_paycode_data",
            source="{{result('write_project_paycode_data_mergeid_csv')}}",
            name="paycodedatacollection",
            columns={"entrydate": "entrydate1", "username": "username1", "tradename": "tradename1",
                     "taskname": "taskname1", "employeeid": "employeeid1", "projectname": "projectname1",
                     "projectcode": "projectcode1", "taskcode": "taskcode1", "paycodename": "paycodename1",
                     "paycodecode": "paycodecode1", "paycodehrs": "paycodehrs1", "mergeid": "mergeid1"},
        )

        query_final_data = rail.QueryCollectionOperator(
            task_id="query_final_data",
            query="""SELECT entrydate1,username1,VDCOverrideShift1,OverrideShift1,
                    usersupervisornamecurrent,tradegroupcurrentuserudf,taskname1,paycodename1,paycodecode1,
                    paycodehrs1,eeid,projectname1,projectcode1,taskcode1,
                    approvalstatus, mergeid1 FROM query_supervisor_project_in_report_data, paycodedatacollection WHERE mergeid1 = mergeid"""
        )

        if_merge_project_paycode_has_data = rail.IfOperator(
            task_id="if_merge_project_paycode_has_data",
            test='{{result("query_final_data", "length") > 0}}',
            no_task="send_no_data_mail",
            yes_task="write_project_paycode_data_csv"
        )

        send_no_data_mail = rail.EmailOperator(
            task_id="send_no_data_mail",
            to='{{result("get_requestor_details")}}',
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}}' + "| Custom Paycode Report -" +
            '{{ current_time_in_specified_tz(fmt="%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/nodata_mail.html"
        )

        write_project_paycode_data_csv = rail.WriteCSVFileOperator(
            task_id="write_project_paycode_data_csv",
            source='{{result("query_final_data")}}',
            header=["Entry Date", "UserName", "VDC Override Shift - Timesheet Daily field", "Override Shift - Timesheet Daily Field",
                    "User Supervisor Name (Current)", "Trade Group (Current)", "Task Name (Full Path)", "Pay Code Code", "Pay Code Name",
                    "Project Hrs", "EE ID", "Project Name", "Project Code", "Task Code", "Approval Status"],
            row=custom_methods.get_final_export_data
        )

        generate_presigned_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_presigned_download_url",
            artifact_name='{{result("write_project_paycode_data_csv")}}',
            output_file_name="custompaycodeReport_" +
            '{{ current_time_in_specified_tz(fmt="%Y%m%d%H%M%S") }}' + ".csv",
            expires_in_seconds=24*7*60*60
        )

        send_success_email = rail.EmailOperator(
            task_id="send_success_email",
            to='{{result("get_requestor_details")}}',
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}}' + "| Custom Paycode Report -" +
            '{{ current_time_in_specified_tz(fmt="%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/success_mail.html"
        )

        finish = rail.EmptyOperator(task_id="finish")

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}'
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        get_requestor_details >>\
            if_email_present_for_requestor >> rail.Label("No") >>\
            log_to_sumo
        if_email_present_for_requestor >> rail.Label("Yes") >>\
            if_daterange_value_present >> rail.Label("No") >>\
            send_invalid_daterange_mail >> finish
        if_daterange_value_present >> rail.Label("Yes") >>\
            get_assigned_user_permissions >>\
            if_valid_permissions_for_requestor >> rail.Label("No") >>\
            send_invalid_permissions_mail >> finish
        if_valid_permissions_for_requestor >> rail.Label("Yes") >>\
            get_project_hours_report_details >> report_entry >> report_exit >>\
            if_report_generation_successful >> rail.Label("No") >> fail_dagrun
        if_report_generation_successful >> rail.Label("Yes") >>\
            if_report_has_data >> rail.Label("No") >>\
            send_no_data_mail >> finish
        if_report_has_data >> rail.Label("Yes") >>\
            if_payload_has_appropriate_columns >> rail.Label(
                "No") >> fail_dagrun
        if_payload_has_appropriate_columns >> rail.Label("Yes") >> load_project_data_csv >>\
            create_project_data_collection >> query_valid_project_data >>\
            write_valid_project_data_to_csv >> create_validateddata_collection >>\
            query_validated_project_data_with_uniqueid >>\
            if_valid_project_data >> rail.Label(
                "No") >> send_no_data_mail >> finish
        if_valid_project_data >> rail.Label("Yes") >> query_valid_vdc_vsy_shift_data >>\
            write_vdc_vsy_shift_data_csv >> create_vdc_vsy_shift_data_collection >>\
            query_merge_report_data_with_vdc_shift >> query_merge_report_data_without_vdc_shift >>\
            query_create_final_validateddata >>\
            query_supervisor_project_in_report_data >> get_all_paycode_scripts >>\
            create_paycode_download_batch >> run_paycode_download_batch >> wait_for_paycode_batch >>\
            get_payroll_run_batch_result >> download_paycode_batch_file >> load_paycode_file_to_csv >>\
            create_paycode_collection >> query_to_valid_user_paycode_data >>\
            write_project_paycode_data_mergeid_csv >> create_collection_project_paycode_data >>\
            query_final_data >>\
            if_merge_project_paycode_has_data >> rail.Label(
                "No") >> send_no_data_mail >> finish
        if_merge_project_paycode_has_data >> rail.Label("Yes") >>\
            write_project_paycode_data_csv >>\
            generate_presigned_download_url >> send_success_email >>\
            finish >> log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag


rail.for_each_instance(create_main_airflow_dag)
