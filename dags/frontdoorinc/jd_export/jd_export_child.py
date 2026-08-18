from datetime import timedelta
import rail
from frontdoorinc.jd_export.utils import python_callable, request_payload


def create_child_dag(config):
    # pylint: disable=too-many-statements unnecessary-lambda line-too-long
    with rail.create_airflow_dag(
        dag_id=config.jd_export_child_dag_id,
        description=f"Frontdoorinc_JDEIntegration scheduler V3.0 child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_first_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        get_report_input_value = rail.PythonOperator(
            task_id='get_report_input_value',
            python_callable=python_callable.get_report_input_value
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        get_enabled_employee_type_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups'
        )
        get_value_for_contractor_and_full_time = rail.PythonOperator(
            task_id='get_value_for_contractor_and_full_time',
            python_callable=python_callable.get_value_for_contractor_and_full_time
        )

        create_report_generation_batch = rail.RepliconServiceOperator(
            task_id='create_report_generation_batch',
            endpoint="/services/reportService1.svc/CreateReportGenerationBatch",
            data=lambda dag_run: python_callable.get_request_body_payroll_download_batch(dag_run)
        )

        execute_report_generation_batch, wait_for_report_generation_batch = rail.batch_execution(
            'execute_report_generation_batch', create_report_generation_batch.task_id
        )

        get_report_generation_batch_results = rail.RepliconServiceOperator(
            task_id='get_report_generation_batch_results',
            endpoint="/services/reportService1.svc/GetReportGenerationBatchResults",
            data={
                "reportGenerationBatchUri": "{{ result('create_report_generation_batch') }}"
            }
        )

        has_empty_report_data = rail.IfOperator(
            task_id='has_empty_report_data',
            test='''{{ result('get_report_generation_batch_results').reportGenerationResults[0].payload |  starts_with('No Data') }}''',
            yes_task="send_nodata_mail",
            no_task="is_report_has_expected_columns",
        )

        send_nodata_mail = rail.EmailOperator(
            task_id='send_nodata_mail',
            to='{{ dag_run.conf.email }}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Replicon to JDE custom export - no records to be export - {{ result("get_report_input_value").dag_trigger_time }}',
            html_content="templates/emails/no_data_mail.html",
            params= {
                'start_date': "{{ dag_run.conf.start_date }}",
                'end_date': "{{ dag_run.conf.end_date }}"
            }
        )

        expected_report_columns = 'Project Code,IT Financial Budget ID,Project Name,Company,Company Code,Employee ID,User Name,Employee Cost Center,Employee Type,Job Profile Name,User Supervisor Name (Current),Total Hrs,Hourly Cost Amount,Total cost,Approval Status,Entry Date,Month (Entry Date),Project Capitalizable,SaaS Solution,Project Cost Center Code,Finance Department,Employee Cost Center Code,CapEx/OpEx'
        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            test="{{ result('get_report_generation_batch_results').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % expected_report_columns,
            yes_task="parse_csv",
            no_task="finish",
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('get_report_generation_batch_results').reportGenerationResults[0].payload }}",
            headers=[
                'Project Code','IT Financial Budget ID','Project Name','Company','Company Code','Employee ID','User Name',
                'Employee Cost Center','Employee Type','Job Profile Name','User Supervisor Name (Current)','Total Hrs',
                'Hourly Cost Amount','Total cost','Approval Status','Entry Date','Month (Entry Date)','Project Capitalizable',
                'SaaS Solution','Project Cost Center Code','Finance Department','Employee Cost Center Code','CapEx/OpEx'
            ]
        )

        get_export_list1 = rail.WriteCSVFileOperator(
            task_id="get_export_list1",
            source="{{ result('parse_csv') }}",
            header=[
                'projectid',
                'itfinancialbudgetid',
                'projectname',
                'companyname',
                'companyid',
                'employeeid',
                'username',
                'employeecostcentercode',
                'employeetype',
                'jobprofilename',
                'supervisor',
                'total',
                'hourlycost',
                'totalcost',
                'approvalstatus',
                'timesheetentrydate',
                'timesheetentrymonth',
                'projectcapexeligible',
                'saasolution',
                'projectcostcentercode',
                'financialdepartment',
                'opexdebitaccount',
                'opexdebitamount',
                'opexcreditaccount',
                'opexcreditamount',
                'wipaccount(debit)',
                'wipdebitamount',
                'capitalcreditaccount',
                'capitalcreditamount',
                'saasimplaccountwipddebitaccount',
                'saaswipdebitamount',
                'saascapitalcreditaccount',
                'naffundedcreditamount',
                'opexdebittorestructuringgross',
                'opexrestrucutingallocationdebit',
                'opexcredittorelieveit',
                'opexrestructuringallocationcredit'
            ],
            thread_pool_size=10,
            row=python_callable.get_export_list1
        )

        get_export_list2 = rail.WriteCSVFileOperator(
            task_id="get_export_list2",
            source="{{ result('get_export_list1') }}",
            header=[
                'projectid',
                'itfinancialbudgetid',
                'projectname',
                'companyname',
                'companyid',
                'employeeid',
                'username',
                'employeecostcentercode',
                'employeetype',
                'jobprofilename',
                'supervisor',
                'total',
                'hourlycost',
                'totalcost',
                'approvalstatus',
                'timesheetentrydate',
                'timesheetentrymonth',
                'projectcapexeligible',
                'saasolution',
                'projectcostcentercode',
                'financialdepartment',
                'opexdebitaccount',
                'opexdebitamount',
                'opexcreditaccount',
                'opexcreditamount',
                'wipaccount(debit)',
                'wipdebitamount',
                'capitalcreditaccount',
                'capitalcreditamount',
                'saasimplaccountwipddebitaccount',
                'saaswipdebitamount',
                'saascapitalcreditaccount',
                'naffundedcreditamount',
                'opexdebittorestructuringgross',
                'opexrestrucutingallocationdebit',
                'opexcredittorelieveit',
                'opexrestructuringallocationcredit'
            ],
            thread_pool_size=10,
            row=lambda item: python_callable.get_export_list2(item, config.opexdebit_account)
        )

        get_export_list3 = rail.WriteCSVFileOperator(
            task_id="get_export_list3",
            source="{{ result('get_export_list2') }}",
            header=[
                'projectid',
                'itfinancialbudgetid',
                'projectname',
                'companyname',
                'companyid',
                'employeeid',
                'username',
                'employeecostcentercode',
                'employeetype',
                'jobprofilename',
                'supervisor',
                'total',
                'hourlycost',
                'totalcost',
                'approvalstatus',
                'timesheetentrydate',
                'timesheetentrymonth',
                'projectcapexeligible',
                'saasolution',
                'projectcostcentercode',
                'financialdepartment',
                'opexdebitaccount',
                'opexdebitamount',
                'opexcreditaccount',
                'opexcreditamount',
                'wipaccount(debit)',
                'wipdebitamount',
                'capitalcreditaccount',
                'capitalcreditamount',
                'saasimplaccountwipddebitaccount',
                'saaswipdebitamount',
                'saascapitalcreditaccount',
                'naffundedcreditamount',
                'opexdebittorestructuringgross',
                'opexrestrucutingallocationdebit',
                'opexcredittorelieveit',
                'opexrestructuringallocationcredit'
            ],
            thread_pool_size=10,
            row=python_callable.get_export_list3
        )

        compose_innotas_file_csv = rail.WriteCSVFileOperator(
            task_id='compose_innotas_file_csv',
            source="{{ result('get_export_list3') }}",
            header=['Project Code','IT Financial Budget ID','Project Name','Company','Company Code','Employee ID',
                    'User Name','Employee Cost Center','Employee Type','Job Profile Name','User Supervisor Name (Current)',
                    'Total Hrs','Hourly Cost Amount','Total cost','Approval Status','Entry Date','Month (Entry Date)',
                    'Project Capitalizable','SaaS Solution','Project Cost Center Code','Finance Department','Opex Debit Account',
                    'Opex Debit Amount','Opex Credit Account','Opex Credit Amount','WIP Account (Debit)','WIP Debit Amount',
                    'Capital Credit Account','Capital Credit Amount','SaaS Impl Account WIP Debit Account','WIP Debit Amount',
                    'Capital Credit Account','NAF Funded Credit Amount'
                    ],
            row=request_payload.get_innotas_file_csv_row_data
        )

        innotas_file_csv_data_update = rail.PythonOperator(
            task_id="innotas_file_csv_data_update",
            python_callable=lambda: rail.write_artifact(rail.read_artifact(
                rail.result("compose_innotas_file_csv")).replace('""""""', '""'))
        )

        compose_je_file_csv = rail.WriteCSVFileOperator(
            task_id='compose_je_file_csv',
            source="{{ result('get_export_list3') }}",
            header=['Project Code','IT Financial Budget ID','Project Name','Company','Company Code','Employee ID',
                    'User Name','Employee Cost Center','Employee Type','Job Profile Name','User Supervisor Name (Current)',
                    'Total Hrs','Hourly Cost Amount','Total cost','Approval Status','Entry Date','Month (Entry Date)',
                    'Project Capitalizable','SaaS Solution','Project Cost Center Code','Finance Department','Opex Debit Account',
                    'Opex Debit Amount','Opex Credit Account','Opex Credit Amount','WIP Account (Debit)','WIP Debit Amount',
                    'Capital Credit Account','Capital Credit Amount','SaaS Impl Account WIP Debit Account','WIP Debit Amount1',
                    'Capital Credit Account1','NAF Funded Credit Amount', 'opexdebittorestructuringgross', 'opexrestrucutingallocationdebit',
                    'opexcredittorelieveit', 'opexrestructuringallocationcredit'
                    ],
            row=request_payload.get_je_file_csv_row_data
        )

        create_collection_from_je_file_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_je_file_csv',
            source="{{ result('compose_je_file_csv') }}",
            name="innotaslist",
            columns={
                'Project Code': 'projectcode',
                'IT Financial Budget ID':'itfinancialbudgetid',
                'Project Name':'projectname',
                'Company':'company',
                'Company Code':'companycode',
                'Employee ID':'employeeid',
                'User Name':'username',
                'Employee Cost Center':'employeecostcenter',
                'Employee Type':'employeetype',
                'Job Profile Name':'jobprofilename',
                'User Supervisor Name (Current)':'supervisorname',
                'Total Hrs':'totalhrs',
                'Hourly Cost Amount':'hourlycostamount',
                'Total cost':'totalcost',
                'Approval Status':'approvalstatus',
                'Entry Date':'entrydate',
                'Month (Entry Date)':'month',
                'Project Capitalizable':'projectcapitalizable',
                'SaaS Solution':'saassolution',
                'Project Cost Center Code':'projectcostcentercode',
                'Finance Department':'financedepartment',
                'Opex Debit Account':'opexdebitaccount',
                'Opex Debit Amount':'opexdebitamount',
                'Opex Credit Account':'opexcreditaccount',
                'Opex Credit Amount':'oexcreditamount',
                'WIP Account (Debit)':'wipaccountdebit',
                'WIP Debit Amount':'wipdebitamount',
                'Capital Credit Account':'capitalcreditaccount',
                'Capital Credit Amount':'capitalcreditamount',
                'SaaS Impl Account WIP Debit Account':'saasimplaccountwipdebitaccount',
                'WIP Debit Amount1':'wipdebitamountsaas',
                'Capital Credit Account1':'capitalcreditaccountnaf',
                'NAF Funded Credit Amount':'naffundedcreditamount',
                'opexdebittorestructuringgross':'opexdebittorestructuringgrossallocation',
                'opexrestrucutingallocationdebit':'opexrestrucutingallocationdebit',
                'opexcredittorelieveit':'opexcredittorelieveitlaborallocationaccount',
                'opexrestructuringallocationcredit':'opexrestructuringallocationcredit',
            }
        )

        query_je_file_entries = rail.QueryCollectionOperator(
            task_id='query_je_file_entries',
            query="""Select opexdebitaccount as businessunitobjsub, opexdebitamount as amount, projectname, projectcode, financedepartment from innotaslist UNION ALL
                    Select opexcreditaccount as businessunitobjsub, oexcreditamount as amount, projectname,  projectcode,financedepartment from innotaslist UNION ALL
                    Select wipaccountdebit as businessunitobjsub, wipdebitamount as amount, projectname, projectcode, financedepartment from innotaslist UNION ALL
                    Select capitalcreditaccount as businessunitobjsub, capitalcreditamount as amount, projectname, projectcode, financedepartment from innotaslist UNION ALL
                    Select saasimplaccountwipdebitaccount as businessunitobjsub, wipdebitamountsaas as amount, projectname, projectcode, financedepartment from innotaslist UNION ALL
                    Select capitalcreditaccountnaf as businessunitobjsub, naffundedcreditamount as amount, projectname, projectcode, financedepartment from innotaslist""",
            name='rawdatacollectionjefile'
        )

        create_jelist1_data_collection = rail.CreateCollectionOperator(
            task_id='create_jelist1_data_collection',
            source="{{ result('query_je_file_entries') }}",
            name='jelist1'
        )

        query_jelist1_entries = rail.QueryCollectionOperator(
            task_id='query_jelist1_entries',
            query="""SELECT DISTINCT businessunitobjsub, projectname, projectcode FROM jelist1""",
            name='rawdatajelist1'
        )

        create_summary_list_lookup_table = rail.CreateLogOperator(
            task_id='create_summary_list_lookup_table'
        )

        process_jslist1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jslist1',
            items="{{result('query_jelist1_entries')}}",
            trigger_dag_id=config.jd_export_process_jelist_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index: {
                "parent_index": index,
                "businessunitobjsub": item['businessunitobjsub'],
                "projectname": item['projectname'],
                "projectcode": item['projectcode'],
                "summary_list_lookup_table": rail.result('create_summary_list_lookup_table'),
                "je_file_entries": rail.result('query_je_file_entries'),
                "jelist1": rail.result('create_jelist1_data_collection')
            }
        )

        wait_for_process_jslist1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_jslist1',
            dag_runs='{{ result("process_jslist1") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_je_file_csv = rail.WriteCSVFileOperator(
            task_id='create_je_file_csv',
            source="{{ result('create_summary_list_lookup_table') }}",
            header=['Business unit.obj.sub','Amount','Description','Remark','t.obj.sub','0','Financial Department'],
            row=request_payload.get_create_je_file_csv_data
        )

        csv_data_update = rail.PythonOperator(
            task_id="csv_data_update",
            python_callable=lambda: rail.write_artifact(rail.read_artifact(
                rail.result("create_je_file_csv")).replace('""""""', '""'))
        )

        generate_innotas_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_innotas_download_link',
            artifact_name="{{ result('innotas_file_csv_data_update')}}",
            output_file_name='{{result("get_report_input_value").filename}}',
            expires_in_seconds=7*24*60*60,
        )

        send_jd_export_successfull_innotas_mail = rail.EmailOperator(
            task_id='send_jd_export_successfull_innotas_mail',
            to='{{ dag_run.conf.email }}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }}  | JDE custom export successfully completed(Innotas) - {{ result("get_report_input_value").dag_trigger_time }}',
            html_content="templates/emails/send_jd_export_successfull_innotas_mail.html",
            params= {
                'start_date': "{{ dag_run.conf.start_date }}",
                'end_date': "{{ dag_run.conf.end_date }}"
            }
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('csv_data_update')}}",
            output_file_name='{{result("get_report_input_value").filename2}}',
            expires_in_seconds=7*24*60*60,
        )

        send_jd_export_successfull_mail = rail.EmailOperator(
            task_id='send_jd_export_successfull_mail',
            to='{{ dag_run.conf.email }}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }}  | JDE custom export successfully completed(JE)- {{ result("get_report_input_value").dag_trigger_time }}',
            html_content="templates/emails/send_jd_export_successfull_mail.html",
            params= {
                'start_date': "{{ dag_run.conf.start_date }}",
                'end_date': "{{ dag_run.conf.end_date }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_input_value >> get_report_details >> get_enabled_employee_type_groups >> get_value_for_contractor_and_full_time >> \
        create_report_generation_batch >> execute_report_generation_batch
        wait_for_report_generation_batch >> get_report_generation_batch_results >> has_empty_report_data >> rail.Label("Yes") >> send_nodata_mail >> finish
        has_empty_report_data  >> rail.Label("No") >> is_report_has_expected_columns
        is_report_has_expected_columns  >> rail.Label("Yes") >> parse_csv >> get_export_list1
        is_report_has_expected_columns  >> rail.Label("No") >> finish
        get_export_list1 >> get_export_list2 >> get_export_list3 >> compose_innotas_file_csv >> innotas_file_csv_data_update >> compose_je_file_csv >> create_collection_from_je_file_csv
        create_collection_from_je_file_csv >> query_je_file_entries >> create_jelist1_data_collection >> query_jelist1_entries >> \
        create_summary_list_lookup_table >> process_jslist1 >> wait_for_process_jslist1 >> create_je_file_csv >> \
        csv_data_update >> generate_innotas_download_link >> send_jd_export_successfull_innotas_mail >> generate_download_link >> \
        send_jd_export_successfull_mail >> finish

    return dag


rail.for_each_instance(create_child_dag)
