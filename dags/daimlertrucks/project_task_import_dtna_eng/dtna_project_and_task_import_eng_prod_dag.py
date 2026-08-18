
from datetime import timedelta, datetime, timezone
import hashlib
from airflow.models import Variable
import rail
from rail.lib.log import get_master_log_artifact_name
from rail.lib.ecid import get_dagrun_ecid

null = None
null_urn = "urn:replicon:list-type:null"


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dtna_projectandtaskimport_eng_prod_{config.instance}',
        description=f'DTNA_Project and Task Import_Eng_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_today_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_today_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_today_4 = rail.PythonOperator(
            task_id='log_today_4',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        has_input_filename_ends_with_csv = rail.IfOperator(
            task_id="has_input_filename_ends_with_csv",
            test='{{ result("new_file_sensor").split(".")[-1] == "csv" if result("new_file_sensor") else False }}',
            yes_task="download_file",
            no_task="archive_file_incorrect_file_format",
        )
        
        archive_file_incorrect_file_format = rail.SFTPMoveFileOperator(
            task_id='archive_file_incorrect_file_format',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/incorrect_file_format_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}_{{ result('get_time_for_file') }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}_{{ result('get_time_for_file') }}"
        )

        list_import_files = rail.SFTPListFilesOperator(
            task_id="list_import_files",
            paths=[config.referance_filepath],
        )

        def has_any_file(result_task_id, input_file_path):
            if not result_task_id or not input_file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)
            if not data:
                return False
            return len(data[input_file_path]) > 0

        has_any_referance_files = rail.IfOperator(
            task_id="has_any_referance_files",
            test=lambda: has_any_file(
                "list_import_files", config.referance_filepath),
            yes_task="download_referance_file",
            no_task="finish"
        )

        download_referance_file = rail.SFTPDownloadFileOperator(
            task_id='download_referance_file',
            remote_filepath=config.referance_filepath + '/' + "New_Reference.csv"
        )

        parse_referance_csv = rail.LoadCSVFileOperator(
            task_id="parse_referance_csv",
            document="{{result('download_referance_file')}}"
        )

        def get_formated_user_row(item):
            return {
                "referencefield": item["referencefield"],
                "projectname": item["projectname"],
                "projectdescription": item["projectdescription"],
                "taskcode": item["taskcode"],
                "taskdescription": item["taskdescription"],
                "ewrcondition": item["ewrcondition"],
                "deptcntlcd": item["deptcntlcd"],
                "jobworktype": item["jobworktype"],
                "projectengineer": item["projectengineer"],
                "status": item["status"]
            }.values()

        create_csv_lines_referance_rawdata = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_referance_rawdata',
            source="{{ result('parse_referance_csv') }}",
            header=['referencefield',
                    'projectname',
                    'projectdescription',
                    'taskcode',
                    'taskdescription',
                    'ewrcondition',
                    'deptcntlcd',
                    'jobworktype',
                    'projectengineer',
                    'status'],
            row=get_formated_user_row
        )

        create_referance_list_collection = rail.CreateCollectionOperator(
            task_id='create_referance_list_collection',
            name="referancelist",
            source="{{result('parse_referance_csv')}}",
            columns={
                'referencefield': 'referencefield',
                'projectname': 'projectname',
                'projectdescription': 'projectdescription',
                'taskcode': 'taskcode',
                'taskdescription': 'taskdescription',
                'ewrcondition': 'ewrcondition',
                'deptcntlcd': 'deptcntlcd',
                'jobworktype': 'jobworktype',
                'projectengineer': 'projectengineer',
                'status': 'status'
            }
        )

        parse_input_csv = rail.LoadCSVFileOperator(
            task_id="parse_input_csv",
            document="{{result('download_file')}}"
        )

        create_input_list_collection = rail.CreateCollectionOperator(
            task_id='create_input_list_collection',
            name="new_file_project_task_list",
            source="{{result('parse_input_csv')}}",
            columns={
                '#Project_Code': 'projectname',
                'Project_Description': 'projectdescription',
                'EWR': 'taskcode',
                'EWR_Description': 'taskdescription',
                'EWR_Condition': 'ewrcondition',
                'EWR_Dept_Control_Code': 'deptcntlcd',
                'Job_Work_Type': 'jobworktype',
                'Project_Engineer': 'projectengineer',
                'Active/Inactive': 'status'
            }
        )

        query_list_input_listof_project_task_11 = rail.QueryCollectionOperator(
            task_id='query_list_input_listof_project_task_11',
            query="""SELECT * FROM  new_file_project_task_list""",
        )

        input_has_any_data = rail.IfOperator(
            task_id='input_has_any_data',
            test='{{ result("query_list_input_listof_project_task_11", "length") == 0 }}',
            yes_task="finish",
            no_task="create_new_referance_data",
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_new_referance_data(collection_task):
            new_referance = []
            new_referance_info = get_data_from_document(
                rail.result(collection_task))
            for referance_data in new_referance_info:
                if referance_data["projectname"].lower() not in ["project name", "project_code", "project name", "#project_code"]:
                    new_referance.append({
                        "referencefield": referance_data["projectname"] + "-" + referance_data["taskcode"],
                        "projectname": referance_data["projectname"],
                        "projectdescription": referance_data["projectdescription"],
                        "taskcode": referance_data["taskcode"],
                        "taskdescription": referance_data["taskdescription"],
                        "ewrcondition": referance_data["ewrcondition"],
                        "deptcntlcd": referance_data["deptcntlcd"],
                        "jobworktype": referance_data["jobworktype"],
                        "projectengineer": referance_data["projectengineer"],
                        "status": referance_data["status"],
                    })

            return new_referance

        create_new_referance_data = rail.PythonOperator(
            task_id='create_new_referance_data',
            python_callable=lambda:  get_new_referance_data(
                'query_list_input_listof_project_task_11')
        )

        create_new_referance_collection_14 = rail.CreateCollectionOperator(
            task_id='create_new_referance_collection_14',
            source=lambda: rail.result('create_new_referance_data'),
            name="input_file_project_task_list",
        )

        query_list_new_listof_project_task_17 = rail.QueryCollectionOperator(
            task_id='query_list_new_listof_project_task_17',
            query="""SELECT  *  FROM  input_file_project_task_list WHERE input_file_project_task_list.referencefield NOT IN ( SELECT DISTINCT  referancelist.referencefield FROM referancelist)""",
        )

        query_list_reference_listof_project_task_19 = rail.QueryCollectionOperator(
            task_id='query_list_reference_listof_project_task_19',
            query="""SELECT  *  FROM  referancelist""",
        )

        def get_new_referance_files(collection_task):
            new_referance1, new_referance2, new_referance3 = [], [], []
            new_referance_info = get_data_from_document(
                rail.result(collection_task))
            index = 0
            for referance_data in new_referance_info:
                index += 1
                if referance_data["projectname"] != "Project Name" and \
                    referance_data["projectname"] != "Project_Code" and \
                        referance_data["projectname"] != "Project Name":
                    if index < 2000:
                        new_referance1.append({
                            "referencefield": referance_data["projectname"] + "-" + referance_data["taskcode"],
                            "projectcode": referance_data["projectname"],
                            "projectdescription": referance_data["projectdescription"],
                            "taskcode": referance_data["taskcode"],
                            "taskdescription": referance_data["taskdescription"],
                            "ewrcondition": referance_data["ewrcondition"],
                            "deptcntlcd": referance_data["deptcntlcd"],
                            "jobworktype": referance_data["jobworktype"],
                            "projectengineer": referance_data["projectengineer"],
                            "status": referance_data["status"],
                        })
                    if index > 1999 < 4000:
                        if referance_data["projectname"] != "Project Name":
                            new_referance2.append({
                                "referencefield": referance_data["projectname"] + "-" + referance_data["taskcode"],
                                "projectcode": referance_data["projectname"],
                                "projectdescription": referance_data["projectdescription"],
                                "taskcode": referance_data["taskcode"],
                                "taskdescription": referance_data["taskdescription"],
                                "ewrcondition": referance_data["ewrcondition"],
                                "deptcntlcd": referance_data["deptcntlcd"],
                                "jobworktype": referance_data["jobworktype"],
                                "projectengineer": referance_data["projectengineer"],
                                "status": referance_data["status"],
                            })
                    if index > 3999 < 6000:
                        if referance_data["projectname"] != "Project Name":
                            new_referance3.append({
                                "referencefield": referance_data["projectname"] + "-" + referance_data["taskcode"],
                                "projectcode": referance_data["projectname"],
                                "projectdescription": referance_data["projectdescription"],
                                "taskcode": referance_data["taskcode"],
                                "taskdescription": referance_data["taskdescription"],
                                "ewrcondition": referance_data["ewrcondition"],
                                "deptcntlcd": referance_data["deptcntlcd"],
                                "jobworktype": referance_data["jobworktype"],
                                "projectengineer": referance_data["projectengineer"],
                                "status": referance_data["status"],
                            })

            return {
                "new_referance1": new_referance1,
                "new_referance2": new_referance2,
                "new_referance3": new_referance3
            }

        get_three_referance_files = rail.PythonOperator(
            task_id='get_three_referance_files',
            python_callable=lambda: get_new_referance_files(
                'query_list_reference_listof_project_task_19')
        )

        def get_matching_column_from_collection_new(project_collection):
            matching_collection = []
            for referance_data in project_collection:
                matching_collection.append({
                    "referencefield": referance_data["referencefield"],
                    "projectname": referance_data["projectname"],
                    "projectdescription": referance_data["projectdescription"],
                    "taskcode": referance_data["taskcode"],
                    "taskdescription": referance_data["taskdescription"],
                    "ewrcondition": referance_data["ewrcondition"],
                    "deptcntlcd": referance_data["deptcntlcd"],
                    "jobworktype": referance_data["jobworktype"],
                    "projectengineer": referance_data["projectengineer"],
                    "status": referance_data["status"],
                    "proj_referance": hashlib.md5((referance_data['referencefield']+","
                                                   + referance_data['projectname']+","
                                                   + referance_data['projectdescription']+","
                                                   + referance_data['taskcode']+","
                                                   + referance_data['taskdescription']+","
                                                   + referance_data['ewrcondition']+","
                                                   + referance_data['deptcntlcd']+","
                                                   + referance_data['jobworktype']+","
                                                   + referance_data['projectengineer']+","
                                                   + referance_data['status']).encode()).hexdigest()
                })
            return matching_collection

        def get_matching_column_from_collection_old(project_collection):
            matching_collection = []
            for referance_data in project_collection:
                matching_collection.append({
                    "referencefield": referance_data["referencefield"],
                    "projectname": referance_data["projectcode"],
                    "projectdescription": referance_data["projectdescription"],
                    "taskcode": referance_data["taskcode"],
                    "taskdescription": referance_data["taskdescription"],
                    "ewrcondition": referance_data["ewrcondition"],
                    "deptcntlcd": referance_data["deptcntlcd"],
                    "jobworktype": referance_data["jobworktype"],
                    "projectengineer": referance_data["projectengineer"],
                    "status": referance_data["status"],
                    "proj_referance": hashlib.md5((referance_data['referencefield']+","
                                                   + referance_data['projectcode']+","
                                                   + referance_data['projectdescription']+","
                                                   + referance_data['taskcode']+","
                                                   + referance_data['taskdescription']+","
                                                   + referance_data['ewrcondition']+","
                                                   + referance_data['deptcntlcd']+","
                                                   + referance_data['jobworktype']+","
                                                   + referance_data['projectengineer']+","
                                                   + referance_data['status']).encode()).hexdigest()
                })
            return matching_collection

        def changed_data_referance_files(referance1, referance2):
            latest_users = get_data_from_document(rail.result(
                referance1)) if rail.result(referance1) else []
            latest_matching_collection = get_matching_column_from_collection_new(
                latest_users)
            old_users = get_data_from_document(rail.result(
                referance2)) if rail.result(referance1) else []
            old_matching_collection = get_matching_column_from_collection_old(
                old_users)
            changed_data = [latest_matching_collection for latest_matching_collection in latest_matching_collection if latest_matching_collection['proj_referance'] not in [
                old_matching_collection["proj_referance"] for old_matching_collection in old_matching_collection]]
            return changed_data

        def unchanged_data_referance_files(referance1, referance2):
            latest_users = get_data_from_document(rail.result(
                referance1)) if rail.result(referance1) else []
            latest_matching_collection = get_matching_column_from_collection_new(
                latest_users)
            old_users = get_data_from_document(rail.result(
                referance2)) if rail.result(referance1) else []
            old_matching_collection = get_matching_column_from_collection_old(
                old_users)
            unchanged_data = [latest_matching_collection for latest_matching_collection in latest_matching_collection if latest_matching_collection['proj_referance'] in [
                old_matching_collection["proj_referance"] for old_matching_collection in old_matching_collection]]
            return unchanged_data

        if_first_referencefield_present_27 = rail.IfOperator(
            task_id='if_first_referencefield_present_27',
            test='''{{ result('get_three_referance_files').new_referance1 | length > 0 }}''',
            yes_task="create_referance1_collection",
            no_task="if_first_referencefield_present_40",
        )

        create_referance1_collection = rail.CreateCollectionOperator(
            task_id='create_referance1_collection',
            source="{{ result('get_three_referance_files').new_referance1 | to_json }}",
            name="input_file_project_task_list1",
        )

        query_list_existing_listof_project_task1_30 = rail.QueryCollectionOperator(
            task_id='query_list_existing_listof_project_task1_30',
            query="""SELECT *  FROM  input_file_project_task_list  WHERE  input_file_project_task_list.referencefield  IN ( SELECT DISTINCT  input_file_project_task_list1.referencefield FROM  input_file_project_task_list1  )""",
        )

        query_list_reference_listof_project_task_31 = rail.QueryCollectionOperator(
            task_id='query_list_reference_listof_project_task_31',
            query="""SELECT  *  FROM  input_file_project_task_list1""",
        )

        get_changed_records_31 = rail.PythonOperator(
            task_id='get_changed_records_31',
            python_callable=lambda: changed_data_referance_files(
                'query_list_existing_listof_project_task1_30', 'query_list_reference_listof_project_task_31')
        )

        get_unchanged_records_31 = rail.PythonOperator(
            task_id='get_unchanged_records_31',
            python_callable=lambda: unchanged_data_referance_files(
                'query_list_existing_listof_project_task1_30', 'query_list_reference_listof_project_task_31')
        )

        if_first_referencefield_present_40 = rail.IfOperator(
            task_id='if_first_referencefield_present_40',
            test='''{{ result('get_three_referance_files').new_referance2 | length > 0 }}''',
            yes_task="create_referance2_collection",
            no_task="if_first_referencefield_present_53",
        )

        create_referance2_collection = rail.CreateCollectionOperator(
            task_id='create_referance2_collection',
            source="{{ result('get_three_referance_files').new_referance2 | to_json }}",
            name="input_file_project_task_list2",
        )

        query_list_existing_listof_project_task2_43 = rail.QueryCollectionOperator(
            task_id='query_list_existing_listof_project_task2_43',
            query="""SELECT *  FROM  input_file_project_task_list  WHERE  input_file_project_task_list.referencefield  IN ( SELECT DISTINCT  input_file_project_task_list2.referencefield FROM  input_file_project_task_list2  )""",
        )

        query_list_reference_listof_project_task2_44 = rail.QueryCollectionOperator(
            task_id='query_list_reference_listof_project_task2_44',
            query="""SELECT  *  FROM  input_file_project_task_list2""",
        )

        get_changed_records_43 = rail.PythonOperator(
            task_id='get_changed_records_43',
            python_callable=lambda: changed_data_referance_files(
                'query_list_existing_listof_project_task2_43', 'query_list_reference_listof_project_task2_44')
        )

        get_unchanged_records_43 = rail.PythonOperator(
            task_id='get_unchanged_records_43',
            python_callable=lambda: unchanged_data_referance_files(
                'query_list_existing_listof_project_task2_43', 'query_list_reference_listof_project_task2_44')
        )

        if_first_referencefield_present_53 = rail.IfOperator(
            task_id='if_first_referencefield_present_53',
            test='''{{ result('get_three_referance_files').new_referance3 | length > 0 }}''',
            yes_task="create_referance3_collection",
            no_task="merge_changed_records_66",
        )

        create_referance3_collection = rail.CreateCollectionOperator(
            task_id='create_referance3_collection',
            source="{{ result('get_three_referance_files').new_referance3 | to_json }}",
            name="input_file_project_task_list3",
        )

        query_list_existing_listof_project_task3_56 = rail.QueryCollectionOperator(
            task_id='query_list_existing_listof_project_task3_56',
            query="""SELECT *  FROM  input_file_project_task_list  WHERE  input_file_project_task_list.referencefield  IN ( SELECT DISTINCT  input_file_project_task_list3.referencefield FROM  input_file_project_task_list3  )""",
        )

        query_list_reference_listof_project_task3_57 = rail.QueryCollectionOperator(
            task_id='query_list_reference_listof_project_task3_57',
            query="""SELECT  *  FROM  input_file_project_task_list3""",
        )

        get_changed_records_56 = rail.PythonOperator(
            task_id='get_changed_records_56',
            python_callable=lambda: changed_data_referance_files(
                'query_list_existing_listof_project_task3_56', 'query_list_reference_listof_project_task3_57')
        )

        get_unchanged_records_56 = rail.PythonOperator(
            task_id='get_unchanged_records_56',
            python_callable=lambda: unchanged_data_referance_files(
                'query_list_existing_listof_project_task3_56', 'query_list_reference_listof_project_task3_57')
        )

        def get_merged_records(task_1, task_2, task_3):
            record_1 = rail.result(task_1) if rail.result(task_1) else []
            record_2 = rail.result(task_2) if rail.result(task_2) else []
            record_3 = rail.result(task_3) if rail.result(task_3) else []
            return record_1 + record_2 + record_3

        merge_changed_records_66 = rail.PythonOperator(
            task_id='merge_changed_records_66',
            python_callable=lambda: get_merged_records(
                'get_changed_records_31', 'get_changed_records_43', 'get_changed_records_56')
        )

        merge_un_changed_records_69 = rail.PythonOperator(
            task_id='merge_un_changed_records_69',
            python_callable=lambda:  get_merged_records(
                'get_unchanged_records_31', 'get_unchanged_records_43', 'get_unchanged_records_56')
        )

        if_query_list_1_rows_greater_than_0_72 = rail.IfOperator(
            task_id='if_query_list_1_rows_greater_than_0_72',
            test='{{ result("query_list_new_listof_project_task_17", "length") > 0 }}',
            yes_task="create_new_referance_data73",
            no_task="if_document_array_greater_than_0_97",
        )

        def get_project_task_list(collection_task):
            new_referance = []
            new_referance_info = get_data_from_document(
                rail.result(collection_task))
            for referance_data in new_referance_info:
                if referance_data["projectname"].lower() not in ["project name", "project_code", "project name", "#project_code"]:
                    new_referance.append({
                        "referencefield": referance_data["projectname"] + "-" + referance_data["taskcode"],
                        "projectname": referance_data["projectname"],
                        "projectdescription": referance_data["projectdescription"],
                        "taskcode": referance_data["taskcode"],
                        "taskdescription": referance_data["taskdescription"],
                        "ewrconditionudf": referance_data["ewrcondition"],
                        "deptcntlcdudf": referance_data["deptcntlcd"],
                        "jobworktypeudf": referance_data["jobworktype"],
                        "projectengineerudf": referance_data["projectengineer"],
                        "status": referance_data["status"],
                    })

            return new_referance

        create_new_referance_data73 = rail.PythonOperator(
            task_id='create_new_referance_data73',
            python_callable=lambda:  get_project_task_list(
                'query_list_new_listof_project_task_17')
        )

        create_new_referance_collection75 = rail.CreateCollectionOperator(
            task_id='create_new_referance_collection75',
            source=lambda: rail.result('create_new_referance_data73'),
            name="unique_project_project_task_list_new_list",
        )

        query_list_unique_listofprojectsin_project_task_78 = rail.QueryCollectionOperator(
            task_id='query_list_unique_listofprojectsin_project_task_78',
            query="""SELECT DISTINCT( unique_project_project_task_list_new_list.projectname), NULL as projectdescription FROM  unique_project_project_task_list_new_list""",
        )

        foreach_query_list_5_79 = rail.ForEachOperator(
            task_id='foreach_query_list_5_79',
            items="{{ result('query_list_unique_listofprojectsin_project_task_78') }}",
            start_task='if_foreach_6_projectname_not_equals_to_projectname_80',
            end_task='foreach_query_list_5_79_end'
        )

        if_foreach_6_projectname_not_equals_to_projectname_80 = rail.IfOperator(
            task_id='if_foreach_6_projectname_not_equals_to_projectname_80',
            test='''{{ result('foreach_query_list_5_79').projectname != 'Project Name'  and result('foreach_query_list_5_79').projectname != '#Project_Code'  and result('foreach_query_list_5_79').projectname != 'Project_Code'  and result('foreach_query_list_5_79').projectname != 'projectname'  and result('foreach_query_list_5_79').projectname != 'ProjectCode' }}''',
            yes_task="search_projects_81",
            no_task="foreach_query_list_5_79_end",
        )

        def get_filtered_data(response, foreach_task_name):
            projectname = rail.result(foreach_task_name)['projectname']
            data = response.json()['d']['rows']
            projectinfo = list(filter(lambda x: x['projectname'] == projectname, map(lambda item: {
                "projecturi": item['cells'][0]['uri'],
                "projectname": item['cells'][0].get('textValue'),
            }, data)))
            return projectinfo[0] if projectinfo else {}

        search_projects_81 = rail.RepliconServiceOperator(
            task_id='search_projects_81',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": rail.result('foreach_query_list_5_79')['projectname'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=lambda response: get_filtered_data(
                response, 'foreach_query_list_5_79')
        )

        log_get_required_project_uri_82 = rail.PythonOperator(
            task_id='log_get_required_project_uri_82',
            python_callable=lambda:  rail.result('search_projects_81')[
                'projecturi'] if rail.result('search_projects_81') else ""
        )

        if_log_12_blank_83 = rail.IfOperator(
            task_id='if_log_12_blank_83',
            test='''{{ result('log_get_required_project_uri_82') | is_falsy }}''',
            yes_task="create_project_84",
            no_task="if_log_12_present_89",
        )

        create_project_84 = rail.RepliconServiceOperator(
            task_id='create_project_84',
            endpoint="/services/ProjectService1.svc/PutProjectInfo2",
            data=lambda: {
                "target": {
                    "name": rail.result('foreach_query_list_5_79')['projectname']
                },
                "projectInfo": {
                    "name": rail.result('foreach_query_list_5_79')['projectname'],
                    "code": null,
                    "description": rail.result('foreach_query_list_5_79')['projectdescription'],
                    "timeEntryDateRange": null,
                    "projectStatusLabel": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:87a216cf-794c-4a3c-ad2e-072b8fdc85d5",
                        "name": null
                    },
                    "percentCompleted": "0",
                    "client": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":client:12",
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "clientRepresentative": null,
                    "program": null,
                    "projectLeader": null,
                    "customFieldValues": [],
                    "isTimeEntryAllowed": "1",
                    "costTypeUri": null,
                    "estimatedHours": null,
                    "estimatedCost": {
                        "amount": "0",
                        "currency": {
                            "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1",
                            "name": null,
                            "symbol": null
                        }
                    },
                    "estimatedExpenses": null,
                    "budget": null,
                    "isProjectLeaderApprovalRequired": "1",
                    "estimationModeUri": null,
                    "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                        "billingRateFrequency": null,
                        "billingRateFrequencyDuration": null,
                        "billingRates": []
                    },
                    "defaultBillingCurrency": null
                }
            }
        )

        update_project_team_member_assignment_assign_d_t_n_a_e_n_gdepartmenttotheproject_85 = rail.RepliconServiceOperator(
            task_id='update_project_team_member_assignment_assign_d_t_n_a_e_n_gdepartmenttotheproject_85',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=lambda: {
                "projectUri": rail.result('create_project_84')['uri'],
                "resourceUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:11",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_allow_time_entry_against_tasks_only_allow_time_entryagainst_task_only_86 = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_allow_time_entryagainst_task_only_86',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('create_project_84').uri }}",
                "allowTimeEntryAgainstTasksOnly": "true"
            }
        )

        update_project_leader_approval_is_required_skip_project_managerapproval_87 = rail.RepliconServiceOperator(
            task_id='update_project_leader_approval_is_required_skip_project_managerapproval_87',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeaderApprovalIsRequired",
            data={
                "projectUri": "{{ result('create_project_84').uri }}",
                "isRequired": "false"
            }
        )

        update_cost_centerfor_project_update_cost_centerfortheproject_88 = rail.RepliconServiceOperator(
            task_id='update_cost_centerfor_project_update_cost_centerfortheproject_88',
            endpoint="/services/ProjectService1.svc/UpdateCostCenter",
            data=lambda: {
                "projectUri": rail.result('create_project_84')['uri'],
                "costCenter": {
                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":cost-center:b9f0df3b-4682-4882-99a5-8f503d428acc",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        if_log_12_present_89 = rail.IfOperator(
            task_id='if_log_12_present_89',
            test='''{{ result('log_get_required_project_uri_82') | is_truthy }}''',
            yes_task="get_project_details_90",
            no_task="foreach_query_list_5_79_end",
        )

        get_project_details_90 = rail.RepliconServiceOperator(
            task_id='get_project_details_90',
            endpoint='/services/ProjectService1.svc/GetProjectDetails',
            data={
                    "projectUri": '{{ result("log_get_required_project_uri_82") }}'
            },
        )

        get_exist_project_description_90 = rail.PythonOperator(
            task_id='get_exist_project_description_90',
            python_callable=lambda:  rail.result('search_projects_81')[
                'projecturi'] if rail.result('search_projects_81') else ""
        )

        if_foreach_6_projectdescription_present_91 = rail.IfOperator(
            task_id='if_foreach_6_projectdescription_present_91',
            test='''{{ result('foreach_query_list_5_79').projectdescription | is_truthy  and result('foreach_query_list_5_79').projectdescription != result("get_project_details_90").Description }}''',
            yes_task="update_description_update_description_92",
            no_task="foreach_query_list_5_79_end",
        )

        update_description_update_description_92 = rail.RepliconServiceOperator(
            task_id='update_description_update_description_92',
            endpoint="/services/ProjectService1.svc/UpdateDescription",
            data={
                "projectUri": "{{ result('get_project_details_90').uri') }}",
                "description": "{{ result('foreach_query_list_5_79').projectdescription }}"
            }
        )

        foreach_query_list_5_79_end = rail.EmptyOperator(
            task_id='foreach_query_list_5_79_end',
        )

        foreach_query_list_1_93 = rail.ForEachOperator(
            task_id='foreach_query_list_1_93',
            items="{{ result('query_list_new_listof_project_task_17') }}",
            start_task='if_foreach_8_referencefield_not_equals_to_referencefield_94',
            end_task='foreach_query_list_1_93_end'
        )

        declare_dag_runs_93 = rail.SetVariableOperator(
            task_id='declare_dag_runs_93',
            name='process_update_dag_runs_93',
            value=[]
        )

        if_foreach_8_referencefield_not_equals_to_referencefield_94 = rail.IfOperator(
            task_id='if_foreach_8_referencefield_not_equals_to_referencefield_94',
            test='''{{ result('foreach_query_list_1_93').referencefield != 'referencefield' }}''',
            yes_task="accumulate_list_items_14_14_95",
            no_task="foreach_query_list_1_93_end",
        )

        accumulate_list_items_14_14_95 = rail.SetVariableOperator(
            task_id='accumulate_list_items_14_14_95',
            name='Processing List',
            append=True,
            value={
                "referencefield": "{{ result('foreach_query_list_1_93').referencefield }}",
                "projectname": "{{ result('foreach_query_list_1_93').projectname }}",
                "projectdescription": "{{ result('foreach_query_list_1_93').projectdescription }}",
                "taskcode": "{{ result('foreach_query_list_1_93').taskcode }}",
                "taskdescription": "{{ result('foreach_query_list_1_93').taskdescription }}",
                "ewrcondtion": "{{ result('foreach_query_list_1_93').ewrcondition }}",
                "deptcntlid": "{{ result('foreach_query_list_1_93').deptcntlcd }}",
                "jobworktype": "{{ result('foreach_query_list_1_93').jobworktype }}",
                "projectengineer": "{{ result('foreach_query_list_1_93').projectengineer }}",
                "activeinactive": "{{ result('foreach_query_list_1_93').status }}"
            }
        )

        trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96',
            retries=0,
            items=[1],
            trigger_dag_id=f'bulk_dtna_taskimport_eng_prod_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "parent_jobid": get_dagrun_ecid(dag_run),
                "projectname": rail.result('foreach_query_list_1_93')['projectname'],
                "taskcode": rail.result('foreach_query_list_1_93')['taskcode'],
                "taskdescription": rail.result('foreach_query_list_1_93')['taskdescription'],
                "ewrconditionudf": rail.result('foreach_query_list_1_93')['ewrcondition'],
                "deptcntlcdudf": rail.result('foreach_query_list_1_93')['deptcntlcd'],
                "jobworktypeudf": rail.result('foreach_query_list_1_93')['jobworktype'],
                "projectengineerudf": rail.result('foreach_query_list_1_93')['projectengineer'],
                "status": int(rail.result('foreach_query_list_1_93')['status'])
            }
        )

        insert_dag_id_dag_run_list_96 = rail.SetVariableOperator(
            task_id='insert_dag_id_dag_run_list_96',
            append=True,
            name='{{ result("declare_dag_runs_93").name }}',
            value='{{(result("trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96"))[0]}}'
        )

        foreach_query_list_1_93_end = rail.EmptyOperator(
            task_id='foreach_query_list_1_93_end',
        )

        wait_for_completion_trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_dag_id_dag_run_list_96").value | to_json }}'
        )

        if_document_array_greater_than_0_97 = rail.IfOperator(
            task_id='if_document_array_greater_than_0_97',
            test="{{ result('merge_changed_records_66') | length > 0}}",
            yes_task="processing_list_changed_records",
            no_task="if_document_array_greater_than_0_123",
        )

        def get_processing_list_changed_records(changed_record_task):
            processing_list_changed_records = []
            merge_changed_records = rail.result(changed_record_task)
            for record in merge_changed_records:
                if record['referencefield'] != 'referencefield':
                    processing_list_changed_records.append({
                        "referencefield": record['referencefield'],
                        "projectname": record['projectname'],
                        "projectdescription": record['projectdescription'],
                        "taskcode": record['taskcode'],
                        "taskdescription": record['taskdescription'],
                        "ewrcondition": record['ewrcondition'],
                        "deptcntlcd": record['deptcntlcd'],
                        "jobworktype": record['jobworktype'],
                        "projectengineer": record['projectengineer'],
                        "activeinactive": record['status']
                    })

            return processing_list_changed_records

        processing_list_changed_records = rail.PythonOperator(
            task_id='processing_list_changed_records',
            python_callable=lambda:  get_processing_list_changed_records(
                'merge_changed_records_66')
        )

        create_new_referance_collection_102 = rail.CreateCollectionOperator(
            task_id='create_new_referance_collection_102',
            source=lambda: rail.result('processing_list_changed_records'),
            name="unique_project_project_task_list_in_changed_list",
        )

        query_list_unique_listofprojectsin_project_task_103 = rail.QueryCollectionOperator(
            task_id='query_list_unique_listofprojectsin_project_task_103',
            query="""SELECT DISTINCT( unique_project_project_task_list_in_changed_list.projectname) FROM  unique_project_project_task_list_in_changed_list""",
        )

        foreach_query_list_6_104 = rail.ForEachOperator(
            task_id='foreach_query_list_6_104',
            items="{{ result('query_list_unique_listofprojectsin_project_task_103') }}",
            start_task='if_foreach_7_projectname_not_equals_to_projectname_105',
            end_task='foreach_query_list_6_104_end'
        )

        if_foreach_7_projectname_not_equals_to_projectname_105 = rail.IfOperator(
            task_id='if_foreach_7_projectname_not_equals_to_projectname_105',
            test='''{{ result('foreach_query_list_6_104').projectname != 'Project Name' and result('foreach_query_list_6_104').projectname != '#Project_Code'  and result('foreach_query_list_6_104').projectname != 'Project_Code'  and result('foreach_query_list_6_104').projectname != 'projectname'  and result('foreach_query_list_6_104').projectname != 'ProjectCode' }}''',
            yes_task="search_projects_106",
            no_task="foreach_query_list_6_104_end",
        )

        search_projects_106 = rail.RepliconServiceOperator(
            task_id='search_projects_106',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": rail.result('foreach_query_list_6_104')['projectname'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=lambda response: get_filtered_data(
                response, 'foreach_query_list_6_104')
        )

        log_get_required_project_uri_107 = rail.PythonOperator(
            task_id='log_get_required_project_uri_107',
            python_callable=lambda:  rail.result('search_projects_106')[
                'projecturi'] if rail.result('search_projects_106') else ""
        )

        if_log_16_blank_108 = rail.IfOperator(
            task_id='if_log_16_blank_108',
            test='''{{ result('log_get_required_project_uri_107') | is_falsy }}''',
            yes_task="create_project_109",
            no_task="if_log_16_present_114",
        )

        create_project_109 = rail.RepliconServiceOperator(
            task_id='create_project_109',
            endpoint="/services/ProjectService1.svc/PutProjectInfo2",
            data=lambda: {
                "target": {
                    "uri": null,
                    "name": rail.result('foreach_query_list_6_104')['projectname'],
                    "code": null,
                    "parameterCorrelationId": null
                },
                "projectInfo": {
                    "name": rail.result('foreach_query_list_6_104')['projectname'],
                    "code": null,
                    "description": "afmig",
                    "timeEntryDateRange": null,
                    "projectStatusLabel": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:87a216cf-794c-4a3c-ad2e-072b8fdc85d5",
                        "name": null
                    },
                    "percentCompleted": "0",
                    "client": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":client:12",
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "clientRepresentative": null,
                    "program": null,
                    "projectLeader": null,
                    "customFieldValues": [],
                    "isTimeEntryAllowed": "1",
                    "costTypeUri": null,
                    "estimatedHours": null,
                    "estimatedCost": {
                        "amount": "0",
                        "currency": {
                            "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":currency:1",
                            "name": null,
                            "symbol": null
                        }
                    },
                    "estimatedExpenses": null,
                    "budget": null,
                    "isProjectLeaderApprovalRequired": "1",
                    "estimationModeUri": null,
                    "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                    "timeAndMaterials": {
                        "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                        "billingRateFrequency": null,
                        "billingRateFrequencyDuration": null,
                        "billingRates": []
                    },
                    "defaultBillingCurrency": null
                }
            }
        )

        update_project_team_member_assignment_assign_d_t_n_a_e_n_gdepartmenttotheproject_110 = rail.RepliconServiceOperator(
            task_id='update_project_team_member_assignment_assign_d_t_n_a_e_n_gdepartmenttotheproject_110',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data=lambda: {
                "projectUri": rail.result('create_project_109')['uri'],
                "resourceUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:11",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_allow_time_entry_against_tasks_only_allow_time_entryagainst_task_only_111 = rail.RepliconServiceOperator(
            task_id='update_allow_time_entry_against_tasks_only_allow_time_entryagainst_task_only_111',
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('create_project_109').uri }}",
                "allowTimeEntryAgainstTasksOnly": "true"
            }
        )

        update_project_leader_approval_is_required_skip_project_managerapproval_112 = rail.RepliconServiceOperator(
            task_id='update_project_leader_approval_is_required_skip_project_managerapproval_112',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeaderApprovalIsRequired",
            data={
                "projectUri": "{{ result('create_project_109').uri }}",
                "isRequired": "false"
            }
        )

        update_cost_centerfor_project_update_cost_centerfortheproject_113 = rail.RepliconServiceOperator(
            task_id='update_cost_centerfor_project_update_cost_centerfortheproject_113',
            endpoint="/services/ProjectService1.svc/UpdateCostCenter",
            data=lambda: {
                "projectUri": rail.result('create_project_109')['uri'],
                "costCenter": {
                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":cost-center:b9f0df3b-4682-4882-99a5-8f503d428acc",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        if_log_16_present_114 = rail.IfOperator(
            task_id='if_log_16_present_114',
            test='''{{ result('log_get_required_project_uri_107') | is_truthy }}''',
            yes_task="get_project_details_115",
            no_task="foreach_query_list_6_104_end",
        )

        get_project_details_115 = rail.RepliconServiceOperator(
            task_id='get_project_details_115',
            endpoint='/services/ProjectService1.svc/GetProjectDetails',
            data={
                    "projectUri": '{{ result("log_get_required_project_uri_107") }}'
            },
        )

        update_cost_centerfor_project_update_cost_centerfortheproject_116 = rail.RepliconServiceOperator(
            task_id='update_cost_centerfor_project_update_cost_centerfortheproject_116',
            endpoint="/services/ProjectService1.svc/UpdateCostCenter",
            data=lambda: {
                "projectUri": rail.result('get_project_details_115')['uri'],
                "costCenter": {
                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":cost-center:b9f0df3b-4682-4882-99a5-8f503d428acc",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        def is_project_description_same():
            project_description = rail.result(
                'foreach_query_list_6_104').get('projectdescription')
            return bool(project_description and project_description != rail.result('get_project_details_115')['description'])

        if_foreach_7_projectdescription_present_117 = rail.IfOperator(
            task_id='if_foreach_7_projectdescription_present_117',
            test=is_project_description_same,
            yes_task="update_description_update_description_118",
            no_task="foreach_query_list_6_104_end",
        )

        update_description_update_description_118 = rail.RepliconServiceOperator(
            task_id='update_description_update_description_118',
            endpoint="/services/ProjectService1.svc/UpdateDescription",
            data={
                "projectUri": "{{ result('get_project_details_115').uri }}",
                "description": "{{ result('foreach_query_list_6_104').projectdescription}}"
            }
        )

        foreach_query_list_6_104_end = rail.EmptyOperator(
            task_id='foreach_query_list_6_104_end',
        )

        declare_dag_runs_119 = rail.SetVariableOperator(
            task_id='declare_dag_runs_119',
            name='process_update_dag_runs_119',
            value=[]
        )

        foreach_document_119 = rail.ForEachOperator(
            task_id='foreach_document_119',
            items="{{ result('merge_changed_records_66') | to_json }}",
            start_task='if_foreach_9_referencefield_not_equals_to_referencefield_120',
            end_task='foreach_document_119_end'
        )

        if_foreach_9_referencefield_not_equals_to_referencefield_120 = rail.IfOperator(
            task_id='if_foreach_9_referencefield_not_equals_to_referencefield_120',
            test='''{{ result('foreach_document_119').referencefield != 'referencefield' }}''',
            yes_task="accumulate_list_items_14_14_121",
            no_task="foreach_document_119_end",
        )

        accumulate_list_items_14_14_121 = rail.SetVariableOperator(
            task_id='accumulate_list_items_14_14_121',
            name='Processing List',
            append=True,
            value={
                "referencefield": "{{ result('foreach_document_119').referencefield }}",
                "projectname": "{{ result('foreach_document_119').projectname }}",
                "projectdescription": "{{ result('foreach_document_119').projectdescription }}",
                "taskcode": "{{ result('foreach_document_119').taskcode }}",
                "taskdescription": "{{ result('foreach_document_119').taskdescription }}",
                "ewrcondtion": "{{ result('foreach_document_119').ewrcondition }}",
                "deptcntlid": "{{ result('foreach_document_119').deptcntlcd }}",
                "jobworktype": "{{ result('foreach_document_119').jobworktype }}",
                "projectengineer": "{{ result('foreach_document_119').projectengineer }}",
                "activeinactive": "{{ result('foreach_document_119').status }}"
            }
        )

        trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122',
            retries=0,
            items=[1],
            trigger_dag_id=f'bulk_dtna_taskimport_eng_prod_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "parent_jobid": get_dagrun_ecid(dag_run),
                "projectname": rail.result('foreach_document_119')['projectname'],
                "taskcode": rail.result('foreach_document_119')['taskcode'],
                "taskdescription": rail.result('foreach_document_119')['taskdescription'],
                "ewrconditionudf": rail.result('foreach_document_119')['ewrcondition'],
                "deptcntlcdudf": rail.result('foreach_document_119')['deptcntlcd'],
                "jobworktypeudf": rail.result('foreach_document_119')['jobworktype'],
                "projectengineerudf": rail.result('foreach_document_119')['projectengineer'],
                "status": int(rail.result('foreach_document_119')['status'])
            }
        )

        insert_dag_id_dag_run_list_122 = rail.SetVariableOperator(
            task_id='insert_dag_id_dag_run_list_122',
            append=True,
            name='{{ result("declare_dag_runs_119").name }}',
            value='{{(result("trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122"))[0]}}'
        )

        foreach_document_119_end = rail.EmptyOperator(
            task_id='foreach_document_119_end',
        )

        wait_for_completion_trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_dag_id_dag_run_list_122").value | to_json }}'
        )

        if_document_array_greater_than_0_123 = rail.IfOperator(
            task_id='if_document_array_greater_than_0_123',
            # pylint: disable=line-too-long
            test='''{{ result('merge_un_changed_records_69') | length > 0  and (result("query_list_new_listof_project_task_17", "length") > 0 or result('merge_changed_records_66') | length > 0) }}''',
            yes_task="merge_log_info_124",
            no_task="archive_old_referance_file",
        )

        def get_log_info(dag_run):
            is_error = False
            combined_logs = []
            child_job_id = get_dagrun_ecid(dag_run)
            context = get_master_log_artifact_name(rail.get_current_context())
            master_log_informations = rail.load_all_records(context)
            for master_log in master_log_informations:
                if master_log['properties']:
                    status_info = master_log['properties'].get('status')
                    if status_info and status_info == "Error":
                        is_error = True
                    combined_logs.append({
                        "projectname": master_log['properties'].get('projectname'),
                        "taskname": master_log['properties'].get('taskname'),
                        "status": master_log['properties'].get('status'),
                        "reason": master_log['properties'].get('reason'),
                        "child_job_id": master_log['properties'].get('child_job_id')
                    })

            unchanged_records = rail.result('merge_un_changed_records_69')
            for unchanged in unchanged_records:
                if unchanged['referencefield'] != 'referencefield':
                    combined_logs.append({
                        "projectname": unchanged['projectname'],
                        "taskname": unchanged['taskcode'],
                        "status": "Ignored",
                        "reason": "",
                        "child_job_id": child_job_id
                    })

            return {"import_logs": combined_logs, "is_error": is_error}

        merge_log_info_124 = rail.PythonOperator(
            task_id='merge_log_info_124',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda dag_run:  get_log_info(dag_run)
        )

        archive_old_referance_file = rail.SFTPMoveFileOperator(
            task_id='archive_old_referance_file',
            existing_filename=config.referance_filepath + '/' + "New_Reference.csv",
            new_filename=config.archive_filepath +
            "/Old_Reference_ProjectTask__{{ result('new_file_sensor') | \
                file_name }}_{{ result('get_time_for_file') }}"
        )

        create_csv_lines_new_referance_rawdata = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_new_referance_rawdata',
            source="{{ result('create_new_referance_collection_14') }}",
            header=['referencefield',
                    'projectname',
                    'projectdescription',
                    'taskcode',
                    'taskdescription',
                    'ewrcondition',
                    'deptcntlcd',
                    'jobworktype',
                    'projectengineer',
                    'status'],
            row=get_formated_user_row
        )

        upload_new_referance_to_sftp_134 = rail.SFTPUploadFileOperator(
            task_id='upload_new_referance_to_sftp_134',
            content="{{ result('create_csv_lines_new_referance_rawdata') }}",
            remote_filepath=config.referance_filepath +
            '/' +
            "New_Reference.csv",
        )

        if_log_processed_files_greater_than_1_136 = rail.IfOperator(
            task_id='if_log_processed_files_128_less_than_1_152',
            test='''{{ result('merge_log_info_124')| is_truthy and result('merge_log_info_124').import_logs | length > 0 }}''',
            yes_task="render_logs_csv",
            no_task="send_mail_153",
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('merge_log_info_124').import_logs | to_json }}",
            header=[
                'projectname',
                'taskname',
                'status',
                'failure/reason',
                'jobid'],
            row=[
                '{{ item | attr_or_default("projectname", "") }}',
                '{{ item | attr_or_default("taskname", "") }}',
                '{{ item | attr_or_default("status", "") }}',
                '{{ item | attr_or_default("reason", "")}}',
                '{{ item | attr_or_default("child_job_id", "") }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        if_error_present_125 = rail.IfOperator(
            task_id='if_error_present_125',
            test='''{{ result('merge_log_info_124').is_error == True }}''',
            yes_task="send_mail_146",
            no_task="send_mail_143",
        )

        send_mail_143 = rail.EmailOperator(
            task_id='send_mail_143',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''DaimlerTrucks- Replicon project and task import completed ''',
            html_content='''<p><strong><em><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></em></strong></p>
            <p>Hello ,</p>
            <p>The project and task sync into Replicon for engineering department has been processed based on filename - {{ result('new_file_sensor') }} on {{ current_time() }}.</p>
            <p>Log has been attached for reference&nbsp;</p><a href="{{ result('generate_download_link') }}">Download log file</a>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_146 = rail.EmailOperator(
            task_id='send_mail_146',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''DaimlerTrucks- Replicon project and task import completed with errors ''',
            html_content='''<p><strong><em><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></em></strong></p>
            <p>Hello ,</p>
            <p>The project and task sync into Replicon for engineering department is completed with errors based on filename - {{ result('new_file_sensor') }} on {{ current_time() }}.</p>
            <p>Log has been attached for reference.</p><a href="{{ result('generate_download_link') }}">Download log file</a>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        send_mail_153 = rail.EmailOperator(
            task_id='send_mail_153',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''DaimlerTrucks- Replicon project and task import ignored ''',
            html_content='''<p><strong><em><span style="font-family: 'Calibri',sans-serif;">This is a automated mail, please don't reply&nbsp;</span></em></strong></p>
            <p>Hello ,</p>
            <p>The project and task sync into Replicon for engineering department has been ignored as there is no updated or new lines in filename - {{ result('new_file_sensor') }} on {{ current_time() }}.</p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> log_today_4 >> has_input_filename_ends_with_csv
        has_input_filename_ends_with_csv >> rail.Label(
            'Yes') >> download_file >> archive_file >> list_import_files >> has_any_referance_files
        has_any_referance_files >> rail.Label('Yes') >> download_referance_file >> parse_referance_csv >> create_csv_lines_referance_rawdata >> create_referance_list_collection >> \
            parse_input_csv >> create_input_list_collection >> query_list_input_listof_project_task_11 >> \
            input_has_any_data
        input_has_any_data >> rail.Label(
            'Yes') >> finish
        input_has_any_data >> rail.Label('No') >> create_new_referance_data >> create_new_referance_collection_14 >> query_list_new_listof_project_task_17 >> \
            query_list_reference_listof_project_task_19 >> get_three_referance_files >> \
            if_first_referencefield_present_27
        if_first_referencefield_present_27 >> rail.Label('Yes') >> create_referance1_collection >> query_list_existing_listof_project_task1_30 >> \
            query_list_reference_listof_project_task_31 >> get_changed_records_31 >> get_unchanged_records_31 >> if_first_referencefield_present_40
        if_first_referencefield_present_27 >> rail.Label(
            'No') >> if_first_referencefield_present_40

        if_first_referencefield_present_40 >> rail.Label('Yes') >> create_referance2_collection >> query_list_existing_listof_project_task2_43 >> query_list_reference_listof_project_task2_44 >> \
            get_changed_records_43 >> get_unchanged_records_43 >> if_first_referencefield_present_53
        if_first_referencefield_present_53 >> rail.Label('Yes') >> create_referance3_collection >> query_list_existing_listof_project_task3_56 >> \
            query_list_reference_listof_project_task3_57 >> get_changed_records_56 >> get_unchanged_records_56 >> merge_changed_records_66 >> \
            merge_un_changed_records_69 >> if_query_list_1_rows_greater_than_0_72
        if_query_list_1_rows_greater_than_0_72 >> rail.Label('Yes') >> create_new_referance_data73 >> create_new_referance_collection75 >> query_list_unique_listofprojectsin_project_task_78 >> \
            foreach_query_list_5_79 >> if_foreach_6_projectname_not_equals_to_projectname_80
        if_foreach_6_projectname_not_equals_to_projectname_80 >> rail.Label('Yes') >> search_projects_81 >> log_get_required_project_uri_82 >> \
            if_log_12_blank_83
        if_log_12_blank_83 >> rail.Label('Yes') >> create_project_84 >> update_project_team_member_assignment_assign_d_t_n_a_e_n_gdepartmenttotheproject_85 >> \
            update_allow_time_entry_against_tasks_only_allow_time_entryagainst_task_only_86 >> update_project_leader_approval_is_required_skip_project_managerapproval_87 >> \
            update_cost_centerfor_project_update_cost_centerfortheproject_88 >> if_log_12_present_89
        if_log_12_present_89 >> rail.Label('Yes') >> get_project_details_90 >> \
            get_exist_project_description_90 >> if_foreach_6_projectdescription_present_91
        if_log_12_present_89 >> rail.Label('No') >> foreach_query_list_5_79_end
        if_foreach_6_projectname_not_equals_to_projectname_80 >> rail.Label(
            'No') >> foreach_query_list_5_79_end
        if_foreach_6_projectdescription_present_91 >> rail.Label(
            'Yes') >> update_description_update_description_92 >> foreach_query_list_5_79_end
        foreach_query_list_5_79 >> foreach_query_list_5_79_end >> declare_dag_runs_93 >> foreach_query_list_1_93 >> if_foreach_8_referencefield_not_equals_to_referencefield_94
        if_foreach_8_referencefield_not_equals_to_referencefield_94 >> rail.Label('Yes') >> accumulate_list_items_14_14_95 >> trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96 >> \
            insert_dag_id_dag_run_list_96 >> foreach_query_list_1_93_end
        foreach_query_list_1_93 >> foreach_query_list_1_93_end >> wait_for_completion_trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_96 >> \
            if_document_array_greater_than_0_97
        if_document_array_greater_than_0_97 >> rail.Label('Yes') >> processing_list_changed_records >> \
            create_new_referance_collection_102 >> query_list_unique_listofprojectsin_project_task_103 >> \
            foreach_query_list_6_104 >> if_foreach_7_projectname_not_equals_to_projectname_105
        if_foreach_6_projectdescription_present_91 >> rail.Label(
            'No') >> foreach_query_list_5_79_end
        if_foreach_8_referencefield_not_equals_to_referencefield_94 >> rail.Label(
            'No') >> foreach_query_list_1_93_end
        if_foreach_7_projectname_not_equals_to_projectname_105 >> rail.Label('Yes') >> search_projects_106 >> log_get_required_project_uri_107 >> \
            if_log_16_blank_108
        if_log_16_blank_108 >> rail.Label('Yes') >> create_project_109 >> update_project_team_member_assignment_assign_d_t_n_a_e_n_gdepartmenttotheproject_110 >>\
            update_allow_time_entry_against_tasks_only_allow_time_entryagainst_task_only_111 >> update_project_leader_approval_is_required_skip_project_managerapproval_112 >> \
            update_cost_centerfor_project_update_cost_centerfortheproject_113 >> if_log_16_present_114
        if_log_16_blank_108 >> rail.Label('No') >> if_log_16_present_114
        if_log_16_present_114 >> rail.Label('Yes') >> get_project_details_115 >> update_cost_centerfor_project_update_cost_centerfortheproject_116 >> \
            if_foreach_7_projectdescription_present_117
        if_foreach_7_projectdescription_present_117 >> rail.Label(
            'Yes') >> update_description_update_description_118 >> foreach_query_list_6_104_end
        foreach_query_list_6_104 >> foreach_query_list_6_104_end >> declare_dag_runs_119 >> foreach_document_119 >> \
            if_foreach_9_referencefield_not_equals_to_referencefield_120
        if_foreach_9_referencefield_not_equals_to_referencefield_120 >> rail.Label('Yes') >> accumulate_list_items_14_14_121 >> \
            trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122 >> insert_dag_id_dag_run_list_122 >> foreach_document_119_end
        foreach_document_119 >> foreach_document_119_end >> wait_for_completion_trigger_dag_run_live_bulk_dtna_task_import_eng_prodasync_122 >> \
            if_document_array_greater_than_0_123
        if_log_16_present_114 >> rail.Label(
            'No') >> foreach_query_list_6_104_end
        if_foreach_7_projectdescription_present_117 >> rail.Label(
            'No') >> foreach_query_list_6_104_end
        if_foreach_7_projectname_not_equals_to_projectname_105 >> rail.Label(
            'No') >> foreach_query_list_6_104_end
        if_foreach_9_referencefield_not_equals_to_referencefield_120 >> rail.Label(
            'No') >> foreach_document_119_end
        if_document_array_greater_than_0_123 >> rail.Label(
            'Yes') >> merge_log_info_124 >> archive_old_referance_file >> \
            create_csv_lines_new_referance_rawdata >> upload_new_referance_to_sftp_134 >> if_log_processed_files_greater_than_1_136
        if_log_processed_files_greater_than_1_136 >> rail.Label('Yes') >>\
            render_logs_csv >> generate_download_link >> if_error_present_125
        if_error_present_125 >> rail.Label(
            'Yes') >> send_mail_146 >> finish
        if_error_present_125 >> rail.Label(
            'No') >> send_mail_143 >> finish
        if_first_referencefield_present_53 >> rail.Label(
            'No') >> merge_changed_records_66
        if_first_referencefield_present_40 >> rail.Label(
            'No') >> if_first_referencefield_present_53
        if_document_array_greater_than_0_123 >> rail.Label(
            'No') >> archive_old_referance_file
        has_any_referance_files >> rail.Label('No') >> finish
        has_input_filename_ends_with_csv >> rail.Label(
            'No') >> archive_file_incorrect_file_format >> finish
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        if_document_array_greater_than_0_97 >> rail.Label(
            'No') >> if_document_array_greater_than_0_123
        if_query_list_1_rows_greater_than_0_72 >> rail.Label(
            'No') >> if_document_array_greater_than_0_97
        if_log_12_blank_83 >> rail.Label('No') >> if_log_12_present_89
        if_log_processed_files_greater_than_1_136 >> rail.Label(
            'No') >> send_mail_153 >> finish

    return dag


rail.for_each_instance(create_dag)
