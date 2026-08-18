import hashlib
from datetime import timedelta, datetime
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dairy_lane_client_project_import_master_{config.instance}',
        description=f'DairyLane Client/Project import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_input_file',
            no_task='delete_this_dagrun',
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            new_filename=config.archive_filepath +
            '''{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename='''{{ result('new_file_sensor')}}''',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        if_filename_ends_with_txt = rail.IfOperator(
            task_id='if_filename_ends_with_txt',
            test='''{{ result('new_file_sensor') | ends_with('.txt') }}''',
            yes_task="parse_csv",
            no_task="finish",
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            encoding='utf-8-sig',
            document="{{result('download_file')}}",
            delimiter='|',
            headers=["Client Name", "Client Code", "Client Contact", "Client Contact Email", "Street1", "City1", "State/Province1",
                     "Country1", "Zip/Postal Code1", "Client Phone", "Street2", "City2", "State/Province2", "Country2",
                     "Zip/Postal Code2", "Project Name", "Project Code", "Project Status", "Project Start Date", "Project Level OEF (Drop down)", "Task Name",
                     "Task Name(level 2)"]
        )

        if_parse_csv_has_no_data = rail.IfOperator(
            task_id='if_parse_csv_has_no_data',
            test= lambda: not rail.load_all_records(rail.result('parse_csv')),
            yes_task="send_mail_file_is_blank",
            no_task='compose_csv'
        )

        send_mail_file_is_blank = rail.EmailOperator(
            task_id='send_mail_file_is_blank',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="dairylanesystems: Project/client Import logs: {{ result('new_file_sensor') | file_base}}",
            html_content='templates/file_blank_mail.html',
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('parse_csv') }}",
            delimiter='|',
            header=['Client Name',
                    'Client Code',
                    'Client Contact',
                    'Client Contact Email',
                    'Street1',
                    'City1',
                    'State/Province1',
                    'Country1',
                    'Zip/Postal Code1',
                    'Client Phone',
                    'Street2',
                    'City2',
                    'State/Province2',
                    'Country2',
                    'Zip/Postal Code2',
                    'Project Name',
                    'Project Code',
                    'Project Status',
                    'Project Start Date',
                    'Project Level OEF (Drop down)',
                    'Task Name',
                    'Task Name(level 2)',
                    'md5'],
            row=lambda item:
            [
                item['Client Name'],
                item['Client Code'],
                item['Client Contact'],
                item['Client Contact Email'],
                item['Street1'],
                item['City1'],
                item['State/Province1'],
                item['Country1'],
                item['Zip/Postal Code1'],
                item['Client Phone'],
                item['Street2'],
                item['City2'],
                item['State/Province2'],
                item['Country2'],
                item['Zip/Postal Code2'],
                item['Project Name'],
                item['Project Code'],
                item['Project Status'],
                item['Project Start Date'],
                item['Project Level OEF (Drop down)'],
                item['Task Name'],
                item['Task Name(level 2)'],
                hashlib.md5((str(item['Client Name'])+str(item['Client Code'])+str(item['Client Contact'])+str(item['Client Contact Email'])+
                             str(item['Street1'])+
                             str(item['City1']) +
                             str(item['State/Province1']) +
                             str(item['Country1']) +
                             str(item['Zip/Postal Code1']) +
                             str(item['Client Phone']) +
                             str(item['Street2']) +
                             str(item['City2']) +
                             str(item['State/Province2']) +
                             str(item['Country2']) +
                             str(item['Zip/Postal Code2']) +
                             str(item['Project Name']) +
                             str(item['Project Code']) +
                             str(item['Project Status']) +
                             str(item['Project Start Date']) +
                             str(item['Project Level OEF (Drop down)']) +
                             str(item['Task Name']) +
                             str(item['Task Name(level 2)'])).encode('utf-8')).hexdigest(),
            ]
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source="{{ result('compose_csv') }}",
            name="inputfile",
            columns={
                'Client Name': 'clientname',
                'Client Code': 'clientcode',
                'Client Contact': 'clientcontact',
                'Client Contact Email': 'clientcontactemail',
                'Street1': 'street',
                'City1': 'city',
                'State/Province1': 'stateprovince',
                'Country1': 'country',
                'Zip/Postal Code1': 'zippostalcode',
                'Client Phone': 'clientphone',
                'Street2': 'projectStreet',
                'City2': 'projectCity',
                'State/Province2': 'projectStateProvince',
                'Country2': 'projectCountry',
                'Zip/Postal Code2': 'projectzippostalcode',
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Project Status': 'projectstatus',
                'Project Start Date': 'projectstartdate',
                'Project Level OEF (Drop down)': 'projectleveloef',
                'Task Name': 'taskname',
                'Task Name(level 2)': 'tasknamelevel2',
                'md5': 'md5'
            }
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.input_filepath_reference_file
        )

        load_csv_from_reference_file = rail.LoadCSVFileOperator(
            task_id="load_csv_from_reference_file",
            encoding='utf-8-sig',
            document="{{result('download_reference_file') }}",
            delimiter='|',
            headers=["Client Name", "Client Code", "Client Contact", "Client Contact Email", "Street1", "City1", "State/Province1",
                     "Country1", "Zip/Postal Code1", "Client Phone", "Street2", "City2", "State/Province2", "Country2",
                     "Zip/Postal Code2", "Project Name", "Project Code", "Project Status", "Project Start Date", "Project Level OEF (Drop down)", "Task Name",
                     "Task Name(level 2)"]
        )

        compose_reference_file_csv = rail.WriteCSVFileOperator(
            task_id='compose_reference_file_csv',
            source="{{ result('load_csv_from_reference_file') }}",
            delimiter='|',
            header=['Client Name',
                    'Client Code',
                    'Client Contact',
                    'Client Contact Email',
                    'Street1',
                    'City1',
                    'State/Province1',
                    'Country1',
                    'Zip/Postal Code1',
                    'Client Phone',
                    'Street2',
                    'City2',
                    'State/Province2',
                    'Country2',
                    'Zip/Postal Code2',
                    'Project Name',
                    'Project Code',
                    'Project Status',
                    'Project Start Date',
                    'Project Level OEF (Drop down)',
                    'Task Name',
                    'Task Name(level 2)',
                    'md5'],
            row=lambda item: [
                item['Client Name'],
                item['Client Code'],
                item['Client Contact'],
                item['Client Contact Email'],
                item['Street1'],
                item['City1'],
                item['State/Province1'],
                item['Country1'],
                item['Zip/Postal Code1'],
                item['Client Phone'],
                item['Street2'],
                item['City2'],
                item['State/Province2'],
                item['Country2'],
                item['Zip/Postal Code2'],
                item['Project Name'],
                item['Project Code'],
                item['Project Status'],
                item['Project Start Date'],
                item['Project Level OEF (Drop down)'],
                item['Task Name'],
                item['Task Name(level 2)'],
                hashlib.md5((str(item['Client Name'])+str(item['Client Code'])+str(item['Client Contact'])+str(item['Client Contact Email'])+
                             str(item['Street1'])+
                             str(item['City1']) +
                             str(item['State/Province1']) +
                             str(item['Country1']) +
                             str(item['Zip/Postal Code1']) +
                             str(item['Client Phone']) +
                             str(item['Street2']) +
                             str(item['City2']) +
                             str(item['State/Province2']) +
                             str(item['Country2']) +
                             str(item['Zip/Postal Code2']) +
                             str(item['Project Name']) +
                             str(item['Project Code']) +
                             str(item['Project Status']) +
                             str(item['Project Start Date']) +
                             str(item['Project Level OEF (Drop down)']) +
                             str(item['Task Name']) +
                             str(item['Task Name(level 2)'])).encode('utf-8')).hexdigest(),

            ]
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('compose_reference_file_csv') }}",
            name="referencefile",
            columns={
                'Client Name': 'clientname',
                'Client Code': 'clientcode',
                'Client Contact': 'clientcontact',
                'Client Contact Email': 'clientcontactemail',
                'Street1': 'street',
                'City1': 'city',
                'State/Province1': 'stateprovince',
                'Country1': 'country',
                'Zip/Postal Code1': 'zippostalcode',
                'Client Phone': 'clientphone',
                'Street2': 'projectStreet',
                'City2': 'projectCity',
                'State/Province2': 'projectStateProvince',
                'Country2': 'projectCountry',
                'Zip/Postal Code2': 'projectzippostalcode',
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Project Status': 'projectstatus',
                'Project Start Date': 'projectstartdate',
                'Project Level OEF (Drop down)': 'projectleveloef',
                'Task Name': 'taskname',
                'Task Name(level 2)': 'tasknamelevel2',
                'md5': 'md5'
            }
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  inputfile WHERE  inputfile.md5 IN (SELECT DISTINCT  referencefile.md5 FROM  referencefile)""",
        )

        def get_loggingdetails():
            filename = (rail.result('new_file_sensor').split(
                '/')[-1]).split('.')[0]
            return {
                "timestamp": datetime.utcnow().strftime("%m/%d/%Y"),
                "filename": filename,
                "csvfilename": "/logs_" + filename + ".csv",
                "error": null
            }

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=get_loggingdetails
        )

        get_dairylane_project_import_lookup = rail.CreateLogOperator(
            task_id='get_dairylane_project_import_lookup',
            tenant_wide_name="dairylane_project_import_prod_logs",
            existing_log_mode="append",
        )

        if_unchanged_records_has_data = rail.IfOperator(
            task_id='if_unchanged_records_has_data',
            test="{{ result('query_unchanged_records', 'length') > 0 }}",
            yes_task="insert_logs_for_unchanged_records",
            no_task="query_updated_records",
        )

        insert_logs_for_unchanged_records = rail.WriteLogOperator(
            task_id='insert_logs_for_unchanged_records',
            log="{{ result('get_dairylane_project_import_lookup') }}",
            items = '{{ result("query_unchanged_records")}}',
            message="na",
            severity="Skipped",
            properties={
                "jobid": "{{ dag_run_ecid() }}",
                "projectname": "{{ item.projectname}}",
                "clientname": "{{ item.clientname }}",
                "status": "Skipped",
                "reason": "No change received compared to last file.",
            }
        )

        query_updated_records = rail.QueryCollectionOperator(
            task_id='query_updated_records',
            query="""SELECT * FROM  inputfile WHERE  inputfile.md5 NOT IN (SELECT DISTINCT  referencefile.md5 FROM  referencefile)""",
        )

        if_updated_records_has_no_data = rail.IfOperator(
            task_id='if_updated_records_has_no_data',
            test="{{ result('query_updated_records', 'length') < 1 }}",
            yes_task="search_entries_in_project_import_logs_lookup",
            no_task="if_updated_records_has_data",
        )

        if_updated_records_has_data = rail.IfOperator(
            task_id='if_updated_records_has_data',
            test="{{ result('query_updated_records', 'length') > 0 }}",
            yes_task="get_object_extension_definition_data",
            no_task="finish",
        )

        get_object_extension_definition_data = rail.RepliconServiceOperator(
            task_id='get_object_extension_definition_data',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-definition-list-column:name",
                    "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
                ],
                "sort": [],
                "filterExpression": null
            }
        )

        def get_oef_uri(oefname):
            for item in rail.result('get_object_extension_definition_data')['rows']:
                if item['cells'][0]['textValue'] == oefname:
                    return item['cells'][1]['uri']
            return ''

        get_all_oefs = rail.RepliconServiceOperator(
            task_id='get_all_oefs',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": get_oef_uri(config.address_oef_name)
            }
        )

        def get_payload_for_child(item):
            #pylint: disable=line-too-long
            return {
                "callingdagrunid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
                "clientname": item['clientname'],
                "clientcode": item['clientcode'],
                "clientcontact": item['clientcontact'],
                "clientemail": item['clientcontactemail'],
                "street": item['street'],
                "city": item['city'],
                "stateprovince": item['stateprovince'],
                "country": item['country'],
                "zippostalcode": item['zippostalcode'],
                "clientphone": item['clientphone'],
                "streetbilling": item['projectStreet'],
                "citybilling": item['projectCity'],
                "stateprovincebilling": item['projectStateProvince'],
                "countrybilling": item['projectCountry'],
                "zippostalcodebilling": item['projectzippostalcode'],
                "projectname": item['projectname'],
                "projectcode": item['projectcode'],
                "projectstatus": item['projectstatus'],
                "startdate": item['projectstartdate'],
                "projectoef": item['projectleveloef'],
                "taskname": item['taskname'],
                "tasknamelevel2": item['tasknamelevel2'],
                "filename": (rail.result('new_file_sensor').split('/')[-1]).split('.')[0],
                "startdateday": datetime.strptime(item['projectstartdate'][:10], '%Y-%m-%d').day,
                "startdatemonth": datetime.strptime(item['projectstartdate'][:10], '%Y-%m-%d').month,
                "startdateyear": datetime.strptime(item['projectstartdate'][:10], '%Y-%m-%d').year,
                "projectstatusuri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:project-status-label:{'9e5fdb2a-593c-4d51-b9f9-b2eb31b49f3b' if item['projectstatus'] == 'Open' else 'b535491f-ee8c-4571-b766-c139613003fe'}",
                "addressoefuri": get_oef_uri(config.address_oef_name),
                'clientnameoefuri': get_oef_uri(config.clientname_oef),
                "projectoefvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_oefs')['tags'], 'name', item['projectleveloef'], 'uri', '') if rail.result('get_all_oefs')['name'] else null
            }

        trigger_child_to_import_client_project = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_to_import_client_project',
            retries=0,
            items=lambda: rail.load_all_records(rail.result('query_updated_records')),
            trigger_dag_id=f'dairy_lane_client_project_import_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            #pylint: disable=unnecessary-lambda
            conf=lambda item: get_payload_for_child(item)
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_import_client_project") }}'
        )

        search_entries_in_project_import_logs_lookup = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_project_import_logs_lookup',
            log="{{ result('get_dairylane_project_import_lookup') }}",
            properties={
                'jobid': "{{ dag_run_ecid()}}"
            }
        )

        if_entries_present = rail.IfOperator(
            task_id='if_entries_present',
            test='{{ result("search_entries_in_project_import_logs_lookup") | length > 0}}',
            yes_task="compose_csv_logs",
        )

        compose_csv_logs = rail.WriteCSVFileOperator(
            task_id='compose_csv_logs',
            source='{{result("search_entries_in_project_import_logs_lookup")}}',
            header=['Job ID',
                    'Project Name',
                    'Client Name',
                    'Status',
                    'Reason'],
            row=lambda item: [
                item['properties']['jobid'],
                item['properties']['projectname'].split("|")[0],
                item['properties']['clientname'],
                item['properties']['status'],
                item['properties']['reason'],
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv_logs')}}",
            output_file_name="logs_" +
            "{{ result('new_file_sensor') | file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        def is_failed_entry_present():
            searched_entries = rail.load_all_records(rail.result(
                'search_entries_in_project_import_logs_lookup'))
            entries = [entry['properties'] for entry in searched_entries]
            entry = rail.find_first_by_attr_and_get_attr(
                entries, 'status', 'Failed', 'reason', '')
            return bool(entry)

        if_failed_dag_entry_present = rail.IfOperator(
            task_id='if_failed_dag_entry_present',
            test=is_failed_entry_present,
            yes_task="success_with_errors_mail",
            no_task="success_logs_mail",
        )

        success_with_errors_mail = rail.EmailOperator(
            task_id='success_with_errors_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''dairylanesystems: Project/client Import completed with error Import logs: {{ result('new_file_sensor') | file_base }} ''',
            html_content='templates/success_with_errors_mail.html',
        )

        success_logs_mail = rail.EmailOperator(
            task_id='success_logs_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''dairylanesystems: Project/client Import logs: {{ result('new_file_sensor') | file_base }} ''',
            html_content='templates/success_logs_mail.html',
        )

        upload_log_file = rail.SFTPUploadFileOperator(
            task_id='upload_log_file',
            content='''{{ result('compose_csv_logs') }}''',
            remote_filepath=config.log_filepath +
            '''{{ result('new_file_sensor') | file_name }}''',
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            new_filename=config.archive_filepath +
            '''{{ dag_run_ecid() }}_import_reference.csv''',
            existing_filename=config.input_filepath_reference_file,
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='''{{ result('compose_csv') }}''',
            remote_filepath=config.input_filepath_reference_file,
        )

        delete_entries_from_project_import_lookup = rail.FilterLogEntriesOperator(
            task_id='delete_entries_from_project_import_lookup',
            log="{{ result('get_dairylane_project_import_lookup') }}",
            properties={
                'jobid': "{{ dag_run_ecid()}}"
            },
            remove_filtered_entries=True
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        new_file_sensor >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_input_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> if_filename_ends_with_txt >> rail.Label(
            'Yes') >> parse_csv >> if_parse_csv_has_no_data
        if_filename_ends_with_txt >> rail.Label(
            'No') >> finish
        if_parse_csv_has_no_data >> rail.Label(
            'Yes') >> send_mail_file_is_blank >> finish
        if_parse_csv_has_no_data >> rail.Label(
            'No') >> compose_csv >> create_inputfile_collection >> download_reference_file >> load_csv_from_reference_file >> compose_reference_file_csv
        compose_reference_file_csv >> create_referencefile_collection >> query_unchanged_records >> get_logging_details >> get_dairylane_project_import_lookup
        get_dairylane_project_import_lookup >> if_unchanged_records_has_data
        if_unchanged_records_has_data >> rail.Label(
            'Yes') >> insert_logs_for_unchanged_records >> query_updated_records
        if_unchanged_records_has_data >> rail.Label(
            'No') >> query_updated_records >> if_updated_records_has_no_data
        if_updated_records_has_no_data >> rail.Label(
            'Yes') >> search_entries_in_project_import_logs_lookup
        if_updated_records_has_no_data >> rail.Label(
            'No') >> if_updated_records_has_data
        if_updated_records_has_data >> rail.Label(
            'No') >> finish
        if_updated_records_has_data >> rail.Label(
            'Yes') >> get_object_extension_definition_data >> get_all_oefs >> trigger_child_to_import_client_project >> wait_for_child_dags
        wait_for_child_dags >> search_entries_in_project_import_logs_lookup >> if_entries_present
        if_entries_present >> rail.Label(
            'Yes') >> compose_csv_logs >> generate_download_link
        generate_download_link >> if_failed_dag_entry_present
        if_failed_dag_entry_present >> rail.Label(
            'Yes') >> success_with_errors_mail >> upload_log_file
        if_failed_dag_entry_present >> rail.Label(
            'No') >> success_logs_mail >> upload_log_file >> archive_reference_file >> upload_new_reference_file >> delete_entries_from_project_import_lookup
        delete_entries_from_project_import_lookup >> finish


    return dag


rail.for_each_instance(create_dag)
