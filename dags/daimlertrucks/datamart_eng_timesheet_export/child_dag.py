
from datetime import datetime, timedelta
from airflow.models import Variable
import csv
import rail
from rail.lib.artifact import existing_artifact, new_artifact

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_timesheet_export_child_{config.instance}',
        description=f'Live|Child of DTNA_timesheet_datamart_eng_call_V3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_cut_offdate_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_cut_offdate_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_cut_offdate_3 = rail.PythonOperator(
            task_id='log_cut_offdate_3',
            python_callable=lambda:  {
                'day': 7, 'month': 4, 'year': 2021
            }  # "04/07/2021"
        )

        create_collection_create_list_from_csv_4_4 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4_4',
            source=lambda: rail.get_dag_run_conf()['items'],
            name="batacheddata",
        )

        query_list_get_allraw_datato_process_5 = rail.QueryCollectionOperator(
            task_id='query_list_get_allraw_datato_process_5',
            query="""SELECT * FROM  batacheddata WHERE  NULLIF(batacheddata.status,NULL) IS NOT NULL and  NULLIF(batacheddata.clientworkerid,NULL) IS NOT NULL and  NULLIF(batacheddata.workdate,NULL) IS NOT NULL and  NULLIF(batacheddata.taskid,NULL) IS NOT NULL and  NULLIF(batacheddata.timesheetnumber,NULL) IS NOT NULL  and  NULLIF(batacheddata.periodenddate,'')  IS NOT NULL  and  NULLIF(batacheddata.workdate,NULL) IS NOT NULL  and  NULLIF(batacheddata.projectcode,NULL) IS NOT NULL  and  NULLIF(batacheddata.costcenternumber,NULL) IS NOT NULL  and  NULLIF(batacheddata.hoursworked,NULL) IS NOT NULL  and  CAST(batacheddata.hoursworked as DECIMAL) >= 0 AND NULLIF(batacheddata.timesheeturi,'') IS NOT NULL """,
        )

        supervisor_details_template_6 = rail.RepliconServiceOperator(
            task_id='supervisor_details_template_6',
            endpoint="/services/ReportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ dag_run.conf.supervisor_report_uri }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        parse_csv_7 = rail.LoadCSVFileOperator(
            task_id='parse_csv_7',
            document="{{ result('supervisor_details_template_6').payload }}"
        )

        load_csv_7 = rail.PythonOperator(
            task_id='load_csv_7',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_7'))
        )

        declare_list_8 = rail.SetVariableOperator(
            task_id='declare_list_8',
            append=False,
            name='Rejected List',
            value=[]
        )

        if_query_list_get_allraw_datato_process_5_rows_greater_than_0_9 = rail.IfOperator(
            task_id='if_query_list_get_allraw_datato_process_5_rows_greater_than_0_9',
            test='''{{ result('query_list_get_allraw_datato_process_5','length') > 0 }}''',
            yes_task="create_csv_lines_raw_data_10",
            no_task="if_query_list_process_list_12_rows_greater_than_0_17",
        )

        def get_row_item(item):
            return {
                "column_0": item['status'] or '',
                "column_1": item['workerfirstname'] or '',
                "column_2": item['workerlastname'] or '',
                "column_3": item['clientworkerid'] or '',
                "column_4": item['loginname'] or '',
                "column_5": item['timesheetnumber'] if datetime(**rail.parse_date(item['periodbegindate'], "%m/%d/%Y")) < datetime(**rail.result('log_cut_offdate_3')) else item['timesheeturi'].split(":")[-1],
                "column_6": datetime(**rail.parse_date(item['periodbegindate'], "%m/%d/%Y")).strftime("%m-%d-%Y"),
                "column_7": datetime(**rail.parse_date(item['periodenddate'], "%m/%d/%Y")).strftime("%m-%d-%Y"),
                "column_8": datetime(**rail.parse_date(item['workdate'], "%m/%d/%Y")).strftime("%m-%d-%Y"),
                "column_9": item['projectcode'] or '',
                "column_10": item['projectname'] or '',
                "column_11": item['costcenternumber'] or '',
                "column_12": item['taskid'] or '',
                "column_13": item['taskname'] or '',
                "column_14": f"{float(item['hoursworked']):.2f}",
                "column_15": f"{float(item['weeklyhours']):.2f}",
                "column_16": rail.find_first_by_attr_and_get_attr(rail.result('load_csv_7'), 'User Name', item['approverid'], 'Login Name') or '',
                "column_17": item['hiringmanagerid'] or '',
                "column_18": item['projecttype'] or '',
                "column_19": item['workertype'] or '',
                "column_20": item['timesheeturi'] or '',
                "column_21": ";".join(list(filter(lambda x: x, [
                    "Status include ','" if ", " in (
                        item['status'] or '') else '',
                    "Worker First Name include ','" if ", " in (
                        item['workerfirstname'] or '') else '',
                    "Worker Last Name include ','" if ", " in (
                        item['workerlastname'] or '') else '',
                    "Client Worker Id include ','" if ", " in (
                        item['clientworkerid'] or '') else '',
                    "Login Name include ','" if ", " in (
                        item['loginname'] or '') else '',
                    "Timesheet Number include ','" if ", " in (
                        item['timesheetnumber'] or '') else '',
                    "Project Code include ','" if ", " in (
                        item['projectcode'] or '') else '',
                    "Project Name include ','" if ", " in (
                        item['projectname'] or '') else '',
                    "Cost Center Number include ','" if ", " in (
                        item['costcenternumber'] or '') else '',
                    "Task ID include ','" if ", " in (
                        item['taskid'] or '') else '',
                    "Task Name include ','" if ", " in (
                        item['taskname'] or '') else '',
                    "Hiring Manager ID include ','" if ", " in (
                        item['hiringmanagerid'] or '') else '',
                    "Project Type include ','" if ", " in (
                        item['projecttype'] or '') else '',
                    "Worker Type include ','" if ", " in (
                        item['workertype'] or '') else '',
                ]))),
                "column_22": ";".join(list(filter(lambda x: x, [
                    "Status is greater than 20 characters" if item['status'] and len(
                        item['status']) > 20 else '',
                    "Worker First name is greater than 50 characters" if item['workerfirstname'] and len(
                        item['workerfirstname']) > 50 else '',
                    "Worker Last name is greater than 50 characters" if item['workerlastname'] and len(
                        item['workerlastname']) > 50 else '',
                    "Client Worker ID is greater than 50 characters" if item['clientworkerid'] and len(
                        item['clientworkerid']) > 50 else '',
                    "Login Name is greater than 50 characters" if item['loginname'] and len(
                        item['loginname']) > 50 else '',
                    "Timesheet Number is greater than 19 characters" if item['timesheetnumber'] and len(
                        item['timesheetnumber']) > 19 else '',
                    "Project Code is greater than 50 characters" if item['projectcode'] and len(
                        item['projectcode']) > 50 else '',
                    "Project Name is greater than 100 characters" if item['projectname'] and len(
                        item['projectname']) > 100 else '',
                    "Cost Center Number is greater than 20 characters" if item['costcenternumber'] and len(
                        item['costcenternumber']) > 20 else '',
                    "Task ID is greater than 20 characters" if item['taskid'] and len(
                        item['taskid']) > 20 else '',
                    "Task Name is greater than 100 characters" if item['taskname'] and len(
                        item['taskname']) > 100 else '',
                    "Hours Worked is greater than 9 characters" if item['hoursworked'] and len(
                        item['hoursworked']) > 9 else '',
                    "Weekly Hours is greater than 9 characters" if item['weeklyhours'] and len(
                        item['weeklyhours']) > 9 else '',
                    "Hiring Manager ID is greater than 50 characters" if item['hiringmanagerid'] and len(
                        item['hiringmanagerid']) > 50 else '',
                    "Project Type is greater than 10 characters" if item['projecttype'] and len(
                        item['projecttype']) > 10 else '',
                    "Worker Type is greater than 10 characters" if item['workertype'] and len(
                        item['workertype']) > 20 else '',
                ]))),
                "column_23": "".join(list(filter(lambda x: x, [
                    "Approved ID include ','" if ', ' in (rail.find_first_by_attr_and_get_attr(rail.result(
                        'load_csv_7'), 'User Name', item['approverid'], 'Login Name') or '') else '',
                    "Approved ID is greater than 50 characters" if len(rail.find_first_by_attr_and_get_attr(
                        rail.result('load_csv_7'), 'User Name', item['approverid'], 'Login Name') or '') > 50 else '',
                ]))),
            }.values()

        create_csv_lines_raw_data_10 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_raw_data_10',
            source="{{ result('query_list_get_allraw_datato_process_5') }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type',
                    'Timesheet URI',
                    'check1',
                    'check2',
                    'check3'],
            row=get_row_item
        )

        load_csv_create_list_from_csv_4_11 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_4_11",
            document="{{ result('create_csv_lines_raw_data_10') }}",
        )

        create_collection_create_list_from_csv_4_11 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4_11',
            source="{{ result('load_csv_create_list_from_csv_4_11') }}",
            name="datawithchecks",
            columns={
                'Status': 'status',
                'Worker First Name': 'workerfirstname',
                'Worker Last Name': 'workerlastname',
                'Client Worker ID': 'clientworkerid',
                'Replicon Login Name': 'loginname',
                'Timesheet Number': 'timesheetnumber',
                'Period Begin Date': 'periodbegindate',
                'Period End Date': 'periodenddate',
                'Work Date': 'workdate',
                'Project Code': 'projectcode',
                'Project Name': 'projectname',
                'Cost Center Number': 'costcenternumber',
                'Task ID': 'taskid',
                'Task Name': 'taskname',
                'Hours Worked': 'hoursworked',
                'Weekly Hours': 'weeklyhours',
                'Approver ID': 'approverid',
                'Hiring Manager ID': 'hiringmanagerid',
                'Project Type': 'projecttype',
                'Worker Type': 'workertype',
                'Timesheet URI': 'timesheeturi',
                'check1': 'check1',
                'check2': 'check2',
                'check3': 'check3'
            }
        )

        query_list_process_list_12 = rail.QueryCollectionOperator(
            task_id='query_list_process_list_12',
            query="""SELECT * FROM  datawithchecks WHERE  NULLIF(datawithchecks.check1,'') IS NULL AND   NULLIF(datawithchecks.check2,'') IS NULL AND   NULLIF(datawithchecks.check3,'') IS NULL""",
        )

        query_list_rejected_list_13 = rail.QueryCollectionOperator(
            task_id='query_list_rejected_list_13',
            query="""SELECT * FROM  datawithchecks WHERE   NULLIF(datawithchecks.check1,'') IS NOT NULL OR   NULLIF(datawithchecks.check2,'') IS NOT NULL OR   NULLIF(datawithchecks.check3,'') IS NOT NULL""",
        )

        if_query_list_rejected_list_13_rows_greater_than_0_14 = rail.IfOperator(
            task_id='if_query_list_rejected_list_13_rows_greater_than_0_14',
            test='''{{ result('query_list_rejected_list_13','length') > 0 }}''',
            yes_task="create_csv_lines_15",
            no_task="if_query_list_process_list_12_rows_greater_than_0_17",
        )

        create_csv_lines_15 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_15',
            source="{{ result('query_list_rejected_list_13') }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type',
                    'Reason'],
            row=[
                "{{ item.status }}",
                "{{ item.workerfirstname }}",
                "{{ item.workerlastname }}",
                "{{ item.clientworkerid }}",
                "{{ item.loginname }}",
                "{{ item.timesheetnumber }}",
                "{{ item.periodbegindate }}",
                "{{ item.periodenddate }}",
                "{{ item.workdate }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.costcenternumber }}",
                "{{ item.taskid }}",
                "{{ item.taskname }}",
                "{{ item.hoursworked }}",
                "{{ item.weeklyhours }}",
                "{{ item.approverid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.projecttype }}",
                "{{ item.workertype }}",
                "{{ item.check1 }}{{ item.check2 }}{{ item.check3 }}"
            ],)

        upload_16 = rail.SFTPAppendCSVFileOperator(
            task_id='upload_16',
            content="{{ result('create_csv_lines_15') }}",
            remote_filepath='{{ dag_run.conf.path }}//Processing_Replicon_TimesheetEngr_RejectedRecords_{{ dag_run.conf.todayinmmddyyyyforname }}.csv',
        )

        if_query_list_process_list_12_rows_greater_than_0_17 = rail.IfOperator(
            task_id='if_query_list_process_list_12_rows_greater_than_0_17',
            test='''{{ result('query_list_process_list_12','length') | is_truthy and result('query_list_process_list_12','length') > 0 }}''',
            yes_task="create_csv_lines_process_listpostvalidation_18",
            no_task="if_query_list_final_process_list_23_rows_greater_than_0_28",
        )

        create_csv_lines_process_listpostvalidation_18 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_process_listpostvalidation_18',
            source="{{ result('query_list_process_list_12') }}",
            header=['reference',
                    'status',
                    'workerfirstname',
                    'workerlastname',
                    'clientworkerid',
                    'repliconloginname',
                    'timesheetnumber',
                    'periodbegindate',
                    'periodenddate',
                    'workdate',
                    'projectcode',
                    'projectname',
                    'costcenternumber',
                    'taskid',
                    'taskname',
                    'hoursworked',
                    'weeklyhours',
                    'approverid',
                    'hiringmanagerid',
                    'projecttype',
                    'workertype'],
            row=[
                "{{ item.clientworkerid }}{{ item.workdate }}{{ item.taskid }}",
                "{{ item.status }}",
                "{{ item.workerfirstname }}",
                "{{ item.workerlastname }}",
                "{{ item.clientworkerid }}",
                "{{ item.loginname }}",
                "{{ item.timesheetnumber }}",
                "{{ item.periodbegindate }}",
                "{{ item.periodenddate }}",
                "{{ item.workdate }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.costcenternumber }}",
                "{{ item.taskid }}",
                "{{ item.taskname }}",
                "{{ item.hoursworked }}",
                "{{ item.weeklyhours }}",
                "{{ item.approverid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.projecttype }}",
                "{{ item.workertype }}"
            ],
        )

        load_csv_create_list_from_csv_4_19 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_4_19",
            document="{{ result('create_csv_lines_process_listpostvalidation_18') }}",
        )

        create_collection_create_list_from_csv_4_19 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4_19',
            source="{{ result('load_csv_create_list_from_csv_4_19') }}",
            name="refrencedata",
        )

        query_list_reference_data_20 = rail.QueryCollectionOperator(
            task_id='query_list_reference_data_20',
            query="""SELECT *, EXISTS (SELECT  * FROM refrencedata WHERE reference=master.reference GROUP BY reference HAVING COUNT(*) >1 ) as is_reject FROM  refrencedata AS master""",
        )

        create_csv_lines_listpostduplicatevalidation_21 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_listpostduplicatevalidation_21',
            source="{{ result('query_list_reference_data_20') }}",
            header=['reference',
                    'status',
                    'workerfirstname',
                    'workerlastname',
                    'clientworkerid',
                    'repliconloginname',
                    'timesheetnumber',
                    'periodbegindate',
                    'periodenddate',
                    'workdate',
                    'projectcode',
                    'projectname',
                    'costcenternumber',
                    'taskid',
                    'taskname',
                    'hoursworked',
                    'weeklyhours',
                    'approverid',
                    'hiringmanagerid',
                    'projecttype',
                    'workertype',
                    'reject',
                    'reason'],
            row=lambda item: {
                "column_0": item['reference'],
                "column_1": item['status'],
                "column_2": item['workerfirstname'],
                "column_3": item['workerlastname'],
                "column_4": item['clientworkerid'],
                "column_5": item['repliconloginname'],
                "column_6": item['timesheetnumber'],
                "column_7": item['periodbegindate'],
                "column_8": item['periodenddate'],
                "column_9": item['workdate'],
                "column_10": item['projectcode'],
                "column_11": item['projectname'],
                "column_12": item['costcenternumber'],
                "column_13": item['taskid'],
                "column_14": item['taskname'],
                "column_15": item['hoursworked'],
                "column_16": item['weeklyhours'],
                "column_17": item['approverid'],
                "column_18": item['hiringmanagerid'],
                "column_19": item['projecttype'],
                "column_20": item['workertype'],
                "column_21": "Yes" if item['is_reject'] == '1' else "No",
                "column_22": "Client Worker ID, Work Date and Task ID combination is not unique" if item['is_reject'] == '1' else ''
            }.values()
        )

        load_csv_create_list_from_csv_4_22 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_4_22",
            document="{{ result('create_csv_lines_listpostduplicatevalidation_21') }}",
        )

        create_collection_create_list_from_csv_4_22 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4_22',
            source="{{ result('load_csv_create_list_from_csv_4_22') }}",
            name="validateddatapostduplicatevalidation",
        )

        query_list_final_process_list_23 = rail.QueryCollectionOperator(
            task_id='query_list_final_process_list_23',
            query="""SELECT * FROM  validateddatapostduplicatevalidation WHERE  validateddatapostduplicatevalidation.reject='No'""",
        )

        query_list_reject_list_24 = rail.QueryCollectionOperator(
            task_id='query_list_reject_list_24',
            query="""SELECT * FROM  validateddatapostduplicatevalidation WHERE  validateddatapostduplicatevalidation.reject='Yes'""",
        )

        if_query_list_reject_list_24_rows_greater_than_0_25 = rail.IfOperator(
            task_id='if_query_list_reject_list_24_rows_greater_than_0_25',
            test='''{{result('query_list_reject_list_24','length') > 0}}''',
            yes_task="create_csv_lines_26",
            no_task="if_query_list_final_process_list_23_rows_greater_than_0_28",
        )

        create_csv_lines_26 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_26',
            source="{{ result('query_list_reject_list_24') }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type',
                    'Reason'],
            row=[
                "{{ item.status }}",
                "{{ item.workerfirstname }}",
                "{{ item.workerlastname }}",
                "{{ item.clientworkerid }}",
                "{{ item.repliconloginname }}",
                "{{ item.timesheetnumber }}",
                "{{ item.periodbegindate }}",
                "{{ item.periodenddate }}",
                "{{ item.workdate }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.costcenternumber }}",
                "{{ item.taskid }}",
                "{{ item.taskname }}",
                "{{ item.hoursworked }}",
                "{{ item.weeklyhours }}",
                "{{ item.approverid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.projecttype }}",
                "{{ item.workertype }}",
                "{{ item.reason }}"
            ]
        )

        upload_27 = rail.SFTPAppendCSVFileOperator(
            task_id='upload_27',
            content="{{ result('create_csv_lines_26') }}",
            remote_filepath="{{ dag_run.conf.path }}/Processing_Replicon_TimesheetEngr_RejectedRecords_{{ dag_run.conf.todayinmmddyyyyforname }}.csv",
        )

        if_query_list_final_process_list_23_rows_greater_than_0_28 = rail.IfOperator(
            task_id='if_query_list_final_process_list_23_rows_greater_than_0_28',
            test='''{{ result('query_list_final_process_list_23','length') | is_truthy and result('query_list_final_process_list_23','length') > 0}}''',
            yes_task="create_csv_lines_29",
            no_task="query_list_data_to_reject_31",
        )

        create_csv_lines_29 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_29',
            source="{{ result('query_list_final_process_list_23') }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type'],
            row=[
                "{{ item.status.strip() if item.status else '\n' }}",
                "{{ item.workerfirstname.strip() if item.workerfirstname else '\n' }}",
                "{{ item.workerlastname.strip() if item.workerlastname else '\n' }}",
                "{{ item.clientworkerid.strip() if item.clientworkerid else '\n' }}",
                "{{ item.repliconloginname.strip() if item.repliconloginname else '\n' }}",
                "{{ item.timesheetnumber.strip() if item.timesheetnumber else '\n' }}",
                "{{ item.periodbegindate.strip() if item.periodbegindate else '\n' }}",
                "{{ item.periodenddate.strip() if item.periodenddate else '\n' }}",
                "{{ item.workdate.strip() if item.workdate else '\n' }}",
                "{{ item.projectcode.strip() if item.projectcode else '\n' }}",
                "{{ item.projectname.strip() if item.projectname else '\n' }}",
                "{{ item.costcenternumber.strip() if item.costcenternumber else '\n' }}",
                "{{ item.taskid.strip() if item.taskid else '\n' }}",
                "{{ item.taskname.strip() if item.taskname else '\n' }}",
                "{{ '%0.2f' | format(item.hoursworked | float) }}",
                "{{ '%0.2f' | format(item.weeklyhours | float) }}",
                "{{ item.approverid if item.approverid else '\n' }}",
                "{{ item.hiringmanagerid if item.hiringmanagerid else '\n' }}",
                "{{ item.projecttype if item.projecttype else '\n' }}",
                "{{ item.workertype if item.workertype else '\n' }}"
            ],
            quoting=csv.QUOTE_MINIMAL
        )

        def fix_empty_field_quoting_callable(**context):
            """Replace quoted newlines with empty quotes for selective quoting"""

            # Read the CSV artifact
            csv_artifact_name = rail.result("create_csv_lines_29")

            # Read content as string
            with existing_artifact(csv_artifact_name, mode='r', encoding='utf-8') as input_artifact:
                content = input_artifact.file.read()

            # Replace quoted newlines with empty quotes
            fixed_content = content.replace('"\n"', '""')

            # Write to new artifact
            with new_artifact(mode='w', encoding='utf-8') as output_artifact:
                output_artifact.file.write(fixed_content)
                output_artifact.set_attribute('type', 'csv')
                return output_artifact.name

        log_csv_artifact_for_master_30 = rail.PythonOperator(
            task_id='log_csv_artifact_for_master_30',
            python_callable=fix_empty_field_quoting_callable
        )

        query_list_data_to_reject_31 = rail.QueryCollectionOperator(
            task_id='query_list_data_to_reject_31',
            query="""SELECT * FROM  batacheddata WHERE  NULLIF(batacheddata.timesheeturi,'') IS NOT NULL AND (NULLIF(batacheddata.status,NULL) IS NULL or  NULLIF(batacheddata.clientworkerid,NULL) IS NULL or  NULLIF(batacheddata.workdate,NULL) IS NULL or  NULLIF(batacheddata.taskid,NULL) IS NULL or   NULLIF(batacheddata.timesheetnumber,NULL) IS NULL or  NULLIF(batacheddata.periodenddate,NULL) IS NULL or  NULLIF(batacheddata.projectcode,NULL) IS NULL or  NULLIF(batacheddata.costcenternumber,NULL) IS NULL or  NULLIF(batacheddata.hoursworked,NULL) IS NULL or  batacheddata.hoursworked = "0" )""",
        )

        if_query_list_data_to_reject_31_rows_greater_than_0_32 = rail.IfOperator(
            task_id='if_query_list_data_to_reject_31_rows_greater_than_0_32',
            test='''{{ result('query_list_data_to_reject_31','length') > 0}}''',
            yes_task="create_csv_lines_raw_datafor_datato_reject_33",
            no_task="log_to_sumo",
        )

        create_csv_lines_raw_datafor_datato_reject_33 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_raw_datafor_datato_reject_33',
            source="{{ result('query_list_data_to_reject_31') }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type',
                    'Timesheet URI',
                    'check1',
                    'check2',
                    'check3'],
            row=get_row_item
        )

        load_csv_create_list_from_csv_4_34 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_4_34",
            document="{{ result('create_csv_lines_raw_datafor_datato_reject_33') }}",
        )

        create_collection_create_list_from_csv_4_34 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4_34',
            source="{{ result('load_csv_create_list_from_csv_4_34') }}",
            name="datatoreject",
            columns={
                'Status': 'status',
                'Worker First Name': 'workerfirstname',
                'Worker Last Name': 'workerlastname',
                'Client Worker ID': 'clientworkerid',
                'Replicon Login Name': 'loginname',
                'Timesheet Number': 'timesheetnumber',
                'Period Begin Date': 'periodbegindate',
                'Period End Date': 'periodenddate',
                'Work Date': 'workdate',
                'Project Code': 'projectcode',
                'Project Name': 'projectname',
                'Cost Center Number': 'costcenternumber',
                'Task ID': 'taskid',
                'Task Name': 'taskname',
                'Hours Worked': 'hoursworked',
                'Weekly Hours': 'weeklyhours',
                'Approver ID': 'approverid',
                'Hiring Manager ID': 'hiringmanagerid',
                'Project Type': 'projecttype',
                'Worker Type': 'workertype',
                'Timesheet URI': 'timesheeturi',
                'check1': 'check1',
                'check2': 'check2',
                'check3': 'check3'
            }
        )

        query_list_rejected_list_35 = rail.QueryCollectionOperator(
            task_id='query_list_rejected_list_35',
            query="""SELECT * FROM  datatoreject""",
        )

        if_query_list_rejected_list_35_rows_greater_than_0_36 = rail.IfOperator(
            task_id='if_query_list_rejected_list_35_rows_greater_than_0_36',
            test='''{{result('query_list_rejected_list_35','length') > 0}}''',
            yes_task="create_csv_lines_37",
            no_task="log_to_sumo",
        )

        create_csv_lines_37 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_37',
            source="{{ result('query_list_rejected_list_35') }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type',
                    'Reason'],
            row=[
                "{{ item.status }}",
                "{{ item.workerfirstname }}",
                "{{ item.workerlastname }}",
                "{{ item.clientworkerid }}",
                "{{ item.loginname }}",
                "{{ item.timesheetnumber }}",
                "{{ item.periodbegindate }}",
                "{{ item.periodenddate }}",
                "{{ item.workdate }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.costcenternumber }}",
                "{{ item.taskid }}",
                "{{ item.taskname }}",
                "{{ item.hoursworked }}",
                "{{ item.weeklyhours }}",
                "{{ item.approverid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.projecttype }}",
                "{{ item.workertype }}",
                "{{ item.check1 }}{{ item.check2 }}{{ item.check3 }}"
            ]
        )

        upload_38 = rail.SFTPAppendCSVFileOperator(
            task_id='upload_38',
            content="{{ result('create_csv_lines_37') }}",
            remote_filepath="{{ dag_run.conf.path }}/Processing_Replicon_TimesheetEngr_RejectedRecords_{{ dag_run.conf.todayinmmddyyyyforname }}.csv",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_cut_offdate_3
        log_cut_offdate_3 >> create_collection_create_list_from_csv_4_4 >> query_list_get_allraw_datato_process_5 >> supervisor_details_template_6 >> parse_csv_7 >> load_csv_7 >> declare_list_8 >> if_query_list_get_allraw_datato_process_5_rows_greater_than_0_9
        if_query_list_get_allraw_datato_process_5_rows_greater_than_0_9 >> rail.Label(
            'Yes') >> create_csv_lines_raw_data_10 >> load_csv_create_list_from_csv_4_11 >> create_collection_create_list_from_csv_4_11 >> query_list_process_list_12 >> query_list_rejected_list_13 >> if_query_list_rejected_list_13_rows_greater_than_0_14
        if_query_list_rejected_list_13_rows_greater_than_0_14 >> rail.Label(
            'Yes') >> create_csv_lines_15 >> upload_16 >> if_query_list_process_list_12_rows_greater_than_0_17
        if_query_list_rejected_list_13_rows_greater_than_0_14 >> rail.Label(
            'No') >> if_query_list_process_list_12_rows_greater_than_0_17
        if_query_list_get_allraw_datato_process_5_rows_greater_than_0_9 >> rail.Label(
            'No') >> if_query_list_process_list_12_rows_greater_than_0_17
        if_query_list_process_list_12_rows_greater_than_0_17 >> rail.Label(
            'Yes') >> create_csv_lines_process_listpostvalidation_18 >> load_csv_create_list_from_csv_4_19 >> create_collection_create_list_from_csv_4_19 >> query_list_reference_data_20 >> create_csv_lines_listpostduplicatevalidation_21 >> load_csv_create_list_from_csv_4_22 >> create_collection_create_list_from_csv_4_22 >> query_list_final_process_list_23 >> query_list_reject_list_24 >> if_query_list_reject_list_24_rows_greater_than_0_25
        if_query_list_reject_list_24_rows_greater_than_0_25 >> rail.Label(
            'Yes') >> create_csv_lines_26 >> upload_27 >> if_query_list_final_process_list_23_rows_greater_than_0_28
        if_query_list_reject_list_24_rows_greater_than_0_25 >> rail.Label(
            'No') >> if_query_list_final_process_list_23_rows_greater_than_0_28
        if_query_list_process_list_12_rows_greater_than_0_17 >> rail.Label(
            'No') >> if_query_list_final_process_list_23_rows_greater_than_0_28
        if_query_list_final_process_list_23_rows_greater_than_0_28 >> rail.Label(
            'Yes') >> create_csv_lines_29 >> log_csv_artifact_for_master_30 >> query_list_data_to_reject_31
        if_query_list_final_process_list_23_rows_greater_than_0_28 >> rail.Label(
            'No') >> query_list_data_to_reject_31 >> if_query_list_data_to_reject_31_rows_greater_than_0_32
        if_query_list_data_to_reject_31_rows_greater_than_0_32 >> rail.Label(
            'Yes') >> create_csv_lines_raw_datafor_datato_reject_33 >> load_csv_create_list_from_csv_4_34 >> create_collection_create_list_from_csv_4_34 >> query_list_rejected_list_35 >> if_query_list_rejected_list_35_rows_greater_than_0_36
        if_query_list_rejected_list_35_rows_greater_than_0_36 >> rail.Label(
            'Yes') >> create_csv_lines_37 >> upload_38 >> log_to_sumo
        if_query_list_rejected_list_35_rows_greater_than_0_36 >> rail.Label(
            'No') >> log_to_sumo
        if_query_list_data_to_reject_31_rows_greater_than_0_32 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
