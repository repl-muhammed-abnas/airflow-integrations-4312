
from datetime import timedelta, datetime
from pendulum import datetime as dt
import pytz
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'locktoncompanies_client_import_master_{config.instance}',
        description=f'Lockton_Client_Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        list_files_in_input_folder=rail.SFTPListFilesOperator(
            task_id='list_files_in_input_folder',
            sftp_conn_id=config.sftp_conn_id,
            paths=[config.input_filepath],
        )

        def get_time_in_formats():
            date = datetime.now(pytz.timezone(config.timezone))
            return {
                'format1':date.strftime('%Y-%m-%dT%H:%M:%S'),
                'format2':date.strftime('%m%d%Y%H%M%S')
            }

        log_start_time = rail.PythonOperator(
            task_id = 'log_start_time',
            python_callable=get_time_in_formats
        )

        if_no_files_found=rail.IfOperator(
            task_id='if_no_files_found',
            test=lambda: not(rail.result('list_files_in_input_folder')) or len(rail.result('list_files_in_input_folder')[config.input_filepath]) < 1,
            yes_task="send_mail_no_inputfile_found",
            no_task="get_clients_and_customfields_report_details",
        )

        send_mail_no_inputfile_found=rail.EmailOperator(
            task_id='send_mail_no_inputfile_found',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="Lockton Client Import - No file in SFTP Input folder {{result('log_start_time').format1}}",
            html_content='''templates/no_input_file_found_mail.html''',
        )

        get_clients_and_customfields_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_clients_and_customfields_report_details',
            report_name=config.clientsandcustomfields_report
        )

        get_inputfile_name = rail.PythonOperator(
            task_id = 'get_inputfile_name',
            python_callable=lambda: config.input_filepath + rail.result('list_files_in_input_folder')[config.input_filepath][0]['name']
        )

        run_clientsandcustomfields_report=rail.run_report2(
            group_id='run_clientsandcustomfields_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                    "reportUri": "{{result('get_clients_and_customfields_report_details').uri}}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        get_allclients_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_allclients_report_details',
            report_name=config.allclients_report
        )

        run_allclients_report=rail.run_report2(
            group_id='run_allclients_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_allclients_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_csv_from_allclients_report=rail.LoadCSVFileOperator(
            task_id="load_csv_from_allclients_report",
            delimiter='|',
            document="{{(result('run_allclients_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}",
        )

        create_collection_allclients = rail.CreateCollectionOperator(
            task_id='create_collection_allclients',
            source = "{{ result('load_csv_from_allclients_report') }}",
            name = "allclients",
            columns = {
                'Client Name':'clientname',
                'Client Code':'code',
                'Client Status':'status',
                'ClientUri':'uri'
            }
        )

        if_inputfile_ends_with_csv=rail.IfOperator(
            task_id='if_inputfile_ends_with_csv',
            test='''{{ result('get_inputfile_name') | ends_with('csv') }}''',
            yes_task="download_input_file",
            no_task="log_to_sumo",
        )

        download_input_file=rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('get_inputfile_name') }}"
        )

        delete_input_file_for_archiving = rail.SFTPDeleteFileOperator(
            task_id = 'delete_input_file_for_archiving',
            sftp_conn_id=config.sftp_conn_id,
            existing_filename="{{result('get_inputfile_name')}}"
        )

        upload_input_file_for_archiving = rail.SFTPUploadFileOperator(
            task_id = 'upload_input_file_for_archiving',
            sftp_conn_id=config.sftp_internal_conn_id,
            content="{{result('download_input_file')}}",
            remote_filepath=config.archive_filepath + "{{result('get_inputfile_name') | file_name}}"
        )

        load_csv_input_file=rail.LoadCSVFileOperator(
            task_id="load_csv_input_file",
            delimiter='|',
            encoding='cp1252',
            document="{{result('download_input_file')}}",
        )

        create_collection_input_file = rail.CreateCollectionOperator(
            task_id='create_collection_input_file',
            source = "{{ result('load_csv_input_file')}}",
            name = "inputfile",
            columns = {
                'LocktonMasterID':'locktonmasterID',
                'Client Name':'clientname',
                'Client City':'clientcity',
                'Client State':'clientstate',
                'Client/Prospect/Non-Client':'clientprospectnonclient',
                'Lockton P&C Servicing Office':'locktonpncservicingoffice',
                'P&C Producer':'pncproducer',
                'Benefits Producer':'ebproducer',
                'Retirement Producer':'retirementproducer',
                'Lockton Retirement Servicing Office':'locktonretirementservicingoffice',
                'Parent Company':'parentcompany',
                'Parent D&B Number':'parentdnbnumber',
                'Lockton Benefits Servicing Office':'locktonbenefitsservicingoffice',
                'Client D&B Number':'clientdnbnumber',
                'Client D&B Name':'clientdnbname'
            }
        )

        create_lockton_clientimport_logs_lookuptable = rail.CreateLogOperator(
            task_id = 'create_lockton_clientimport_logs_lookuptable'
        )

        def get_customfield_uris(response):
            customfields = [{
                'name': customfield['cells'][0]['textValue'],
                'uri': customfield['cells'][0]['uri']
            } for customfield in response['rows']]
            return {
                'cf_clientdnbnumber': rail.find_first_by_attr_and_get_attr(customfields,'name','Client D&B Number','uri',''),
                'cf_parentcompany': rail.find_first_by_attr_and_get_attr(customfields,'name','Parent Company','uri',''),
                'cf_parentdnbnumber': rail.find_first_by_attr_and_get_attr(customfields,'name','Parent D&B Number','uri',''),
                'cf_clientdnbname': rail.find_first_by_attr_and_get_attr(customfields,'name','Client D&B Name','uri',''),
                'cf_clientprospectnonclient': rail.find_first_by_attr_and_get_attr(customfields,'name','Client/Prospect/Non-Client','uri',''),
                'cf_locktonbenefitsservicingoffice': rail.find_first_by_attr_and_get_attr(customfields,'name','Lockton Benefits Servicing Office','uri',''),
                'cf_retirementproducers': rail.find_first_by_attr_and_get_attr(customfields,'name','Retirement Producer','uri',''),
                'cf_ebproducer': rail.find_first_by_attr_and_get_attr(customfields,'name','EB Producer','uri',''),
                'cf_pncproducer': rail.find_first_by_attr_and_get_attr(customfields,'name','P&C Producer','uri',''),
                'cf_locktonretirementservicingoffice': rail.find_first_by_attr_and_get_attr(customfields,'name','Lockton Retirement Servicing Office','uri',''),
                'cf_locktonpncservicingoffice': rail.find_first_by_attr_and_get_attr(customfields,'name','Lockton P&C Servicing Office','uri',''),
            }

        get_required_customfield_uris = rail.RepliconServiceOperator(
            task_id = 'get_required_customfield_uris',
            endpoint="/services/ClientCustomFieldListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:client-custom-field-list-column:client-custom-field"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_customfield_uris
        )

        get_dropdown_options_clientprospectnonclient = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_clientprospectnonclient',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_clientprospectnonclient']
            }
        )

        get_dropdown_options_locktonbenefitsservicingoffice = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_locktonbenefitsservicingoffice',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_locktonbenefitsservicingoffice']
            }
        )

        get_dropdown_options_retirementproducers = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_retirementproducers',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_retirementproducers']
            }
        )

        get_dropdown_options_ebproducer = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_ebproducer',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_ebproducer']
            }
        )

        get_dropdown_options_pncproducer = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_pncproducer',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_pncproducer']
            }
        )

        get_dropdown_options_locktonretirementservicingoffice = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_locktonretirementservicingoffice',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_locktonretirementservicingoffice']
            }
        )

        get_dropdown_options_locktonpncservicingoffice = rail.RepliconServiceOperator(
            task_id = 'get_dropdown_options_locktonpncservicingoffice',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                "customFieldUri": rail.result('get_required_customfield_uris')['cf_locktonpncservicingoffice']
            }
        )

        query_clients_tobe_enabled=rail.QueryCollectionOperator(
            task_id='query_clients_tobe_enabled',
            query="""SELECT allclients.uri, inputfile.* FROM allclients INNER JOIN inputfile
                ON allclients.code=inputfile.locktonmasterID AND allclients.status='Disabled'""",
        )

        def get_validclients(clients):
            all_clients = rail.load_all_records(clients)
            return [ client for client in all_clients if ( client['locktonmasterID'] and not client['locktonmasterID'].startswith('_') )]

        trigger_child_dag_enable_clients=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_dag_enable_clients',
            retries=0,
            items=lambda: get_validclients(rail.result('query_clients_tobe_enabled')),
            trigger_dag_id=f'locktoncompanies_client_import_enable_client_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "uri": item['uri'],
                "name": item['clientname'].replace('`','"') if item['clientname'] else '',
                "code": item['locktonmasterID'],
                "logslookuptable": rail.result('create_lockton_clientimport_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_child_enable_clients = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_enable_clients',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_dag_enable_clients") }}'
        )

        download_reference_file=rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_internal_conn_id,
            remote_filepath=config.reference_filepath + 'reference_file.csv'
        )

        load_csv_reference_file=rail.LoadCSVFileOperator(
            task_id="load_csv_reference_file",
            delimiter='|',
            encoding='cp1252',
            document="{{result('download_reference_file')}}",
        )

        create_collection_reference_file = rail.CreateCollectionOperator(
            task_id='create_collection_reference_file',
            source = "{{ result('load_csv_reference_file') }}",
            name = "referencefile",
            columns = {
                'LocktonMasterID':'locktonmasterID',
                'Client Name':'clientname',
                'Client City':'clientcity',
                'Client State':'clientstate',
                'Client/Prospect/Non-Client':'clientprospectnonclient',
                'Lockton P&C Servicing Office':'locktonpncservicingoffice',
                'P&C Producer':'pncproducer',
                'Benefits Producer':'ebproducer',
                'Retirement Producer':'retirementproducer',
                'Lockton Retirement Servicing Office':'locktonretirementservicingoffice',
                'Parent Company':'parentcompany',
                'Parent D&B Number':'parentdnbnumber',
                'Lockton Benefits Servicing Office':'locktonbenefitsservicingoffice',
                'Client D&B Number':'clientdnbnumber',
                'Client D&B Name':'clientdnbname'
            }
        )

        query_deltavalues=rail.QueryCollectionOperator(
            task_id='query_deltavalues',
            name='deltavalues',
            query="""SELECT  inputfile.locktonmasterID,  inputfile.clientname,  inputfile.clientcity,  inputfile.clientstate,
                inputfile.clientprospectnonclient,  inputfile.locktonpncservicingoffice,  inputfile.pncproducer,  inputfile.ebproducer,
                inputfile.retirementproducer,  inputfile.locktonretirementservicingoffice,  inputfile.parentcompany,  inputfile.parentdnbnumber,
                inputfile.locktonbenefitsservicingoffice,  inputfile.clientdnbnumber,  inputfile.clientdnbname FROM inputfile EXCEPT
                SELECT referencefile.locktonmasterID,  referencefile.clientname,  referencefile.clientcity,  referencefile.clientstate,
                referencefile.clientprospectnonclient,  referencefile.locktonpncservicingoffice,  referencefile.pncproducer,  referencefile.ebproducer,
                referencefile.retirementproducer,  referencefile.locktonretirementservicingoffice,  referencefile.parentcompany,
                referencefile.parentdnbnumber,  referencefile.locktonbenefitsservicingoffice,  referencefile.clientdnbnumber,
                referencefile.clientdnbname FROM  referencefile""",
        )

        query_clients_to_be_updated=rail.QueryCollectionOperator(
            task_id='query_clients_to_be_updated',
            query="""SELECT allclients.uri, deltavalues.* FROM allclients INNER JOIN deltavalues
                ON allclients.code=deltavalues.locktonmasterID""",
        )

        def get_records_count(records,match):
            count = 0
            for record in records:
                if record['code'] == match['locktonmasterID']:
                    count+=1
            return count

        trigger_child_update_client=rail.trigger_parallel_dagrun(
            task_id='trigger_child_update_client',
            parallel_count= config.max_active_parallel_runs,
            items=lambda: get_validclients(rail.result('query_clients_to_be_updated')),
            trigger_dag_id=f'locktoncompanies_client_import_update_client_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "ClientURI": item['uri'],
                "ClientCode": item['locktonmasterID'],
                "ClientName": item['clientname'].replace('`','"').strip() if item['clientname'] else '',
                "Clientcity": item['clientcity'],
                "ClientState": item['clientstate'],
                "Clientprospectnonclient": item['clientprospectnonclient'].strip() if item['clientprospectnonclient'] else '',
                "clientprospectnonclientfielduri": rail.result('get_required_customfield_uris')['cf_clientprospectnonclient'],
                "clientprospectnonclientoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_clientprospectnonclient'),'displayText',item['clientprospectnonclient'].strip() if
                    item['clientprospectnonclient'] else '','uri',''),
                "locktonPnCServicingOffice": item['locktonpncservicingoffice'].strip() if item['locktonpncservicingoffice'] else '',
                "locktonpncservicingofficefielduri":rail.result('get_required_customfield_uris')['cf_locktonpncservicingoffice'],
                "locktonpncservicingofficeoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_locktonpncservicingoffice'),'displayText',item['locktonpncservicingoffice'].strip() if
                    item['locktonpncservicingoffice'] else '','uri',''),
                "PnCProducer": item['pncproducer'].strip() if item['pncproducer'] else '',
                "pncproducerfielduri":rail.result('get_required_customfield_uris')['cf_pncproducer'],
                "pncproduceroptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_pncproducer'),'displayText',item['pncproducer'].strip() if item['pncproducer'] else '','uri',''),
                "EBProducer": item['ebproducer'].strip() if item['ebproducer'] else '',
                "ebproducerfielduri":rail.result('get_required_customfield_uris')['cf_ebproducer'],
                "ebproduceroptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_ebproducer'),'displayText',item['ebproducer'].strip() if item['ebproducer'] else '','uri',''),
                "RetirementProducers": item['retirementproducer'].strip() if item['retirementproducer'] else '',
                "retirementproducersfielduri":rail.result('get_required_customfield_uris')['cf_retirementproducers'],
                "retirementproducersoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_retirementproducers'),'displayText',item['retirementproducer'].strip() if
                    item['retirementproducer'] else '','uri',''),
                "LocktonRetirementServicingOffice": item['locktonretirementservicingoffice'].strip() if item['locktonretirementservicingoffice'] else '',
                "locktonretirementservicingofficefielduri":rail.result('get_required_customfield_uris')['cf_locktonretirementservicingoffice'],
                "locktonretirementservicingofficeoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_locktonretirementservicingoffice'),'displayText',item['locktonretirementservicingoffice'].strip() if
                    item['locktonretirementservicingoffice'] else '','uri',''),
                "ParentCompany": item['parentcompany'],
                "parentcompanyfielduri": rail.result('get_required_customfield_uris')['cf_parentcompany'],
                "ParentDnBNumber": item['parentdnbnumber'] or item['clientdnbnumber'],
                "parentdnbnumberfielduri": rail.result('get_required_customfield_uris')['cf_parentdnbnumber'],
                "LocktonBenefitsServicingOffice": item['locktonbenefitsservicingoffice'].strip() if item['locktonbenefitsservicingoffice'] else '',
                "locktonbenefitsservicingofficefielduri":rail.result('get_required_customfield_uris')['cf_locktonbenefitsservicingoffice'],
                "locktonbenefitsservicingofficeoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_locktonbenefitsservicingoffice'),'displayText',item['locktonbenefitsservicingoffice'].strip() if
                    item['locktonbenefitsservicingoffice'] else '','uri',''),
                "ClientDnBNumber": item['clientdnbnumber'],
                "clientdnbnumberfielduri": rail.result('get_required_customfield_uris')['cf_clientdnbnumber'],
                "ClientDnBName": item['clientdnbname'],
                "clientdnbnamefielduri": rail.result('get_required_customfield_uris')['cf_clientdnbname'],
                "results": get_records_count(rail.load_all_records(rail.result('create_collection_allclients')),item),
                "logslookuptable": rail.result('create_lockton_clientimport_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        query_eligible_clients_in_instance=rail.QueryCollectionOperator(
            task_id='query_eligible_clients_in_instance',
            name='eligibleclienttable',
            query="""SELECT * FROM  allclients WHERE  allclients.code NOT LIKE '\\_%' ESCAPE "\\" """,
        )

        query_clients_tobe_disabled=rail.QueryCollectionOperator(
            task_id='query_clients_tobe_disabled',
            query="""SELECT * FROM  eligibleclienttable WHERE  eligibleclienttable.code NOT IN (SELECT  inputfile.locktonmasterID FROM  inputfile) AND
                eligibleclienttable.status='Enabled'""",
        )

        trigger_child_disable_clients=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_disable_clients',
            retries=0,
            items=lambda: rail.load_all_records(rail.result('query_clients_tobe_disabled')),
            trigger_dag_id=f'locktoncompanies_client_import_disable_client_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "ClientURI": item['uri'],
                "clientname": item['clientname'].replace('`','"') if item['clientname'] else '',
                "clientcode": item['code'],
                "logslookuptable": rail.result('create_lockton_clientimport_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        wait_for_child_disable_clients = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_disable_clients',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_disable_clients") }}'
        )

        query_new_clients=rail.QueryCollectionOperator(
            task_id='query_new_clients',
            query="""SELECT * FROM inputfile WHERE NOT EXISTS (SELECT * FROM allclients WHERE allclients.code=inputfile.locktonmasterID)""",
        )

        trigger_child_add_new_clients=rail.trigger_parallel_dagrun(
            task_id='trigger_child_add_new_clients',
            items=lambda: get_validclients(rail.result('query_new_clients')),
            trigger_dag_id=f'locktoncompanies_client_import_add_new_client_{config.instance}',
            parallel_count=config.max_active_parallel_runs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "ClientCode": item['locktonmasterID'],
                "ClientName": item['clientname'].replace('`','"').strip() if item['clientname'] else '',
                "Clientcity": item['clientcity'],
                "ClientState": item['clientstate'],
                "Clientprospectnonclient": item['clientprospectnonclient'].strip() if item['clientprospectnonclient'] else '',
                "clientprospectnonclientfielduri": rail.result('get_required_customfield_uris')['cf_clientprospectnonclient'],
                "clientprospectnonclientoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_clientprospectnonclient'),'displayText',item['clientprospectnonclient'].strip() if
                    item['clientprospectnonclient'] else '','uri',''),
                "locktonPnCServicingOffice": item['locktonpncservicingoffice'].strip() if item['locktonpncservicingoffice'] else '',
                "locktonpncservicingofficefielduri":rail.result('get_required_customfield_uris')['cf_locktonpncservicingoffice'],
                "locktonpncservicingofficeoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_locktonpncservicingoffice'),'displayText',item['locktonpncservicingoffice'].strip() if
                    item['locktonpncservicingoffice'] else '','uri',''),
                "PnCProducer": item['pncproducer'].strip() if item['pncproducer'] else '',
                "pncproducerfielduri":rail.result('get_required_customfield_uris')['cf_pncproducer'],
                "pncproduceroptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_pncproducer'),'displayText',item['pncproducer'].strip() if item['pncproducer'] else '','uri',''),
                "EBProducer": item['ebproducer'].strip() if item['ebproducer'] else '',
                "ebproducerfielduri":rail.result('get_required_customfield_uris')['cf_ebproducer'],
                "ebproduceroptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_ebproducer'),'displayText',item['ebproducer'].strip() if item['ebproducer'] else '','uri',''),
                "RetirementProducers": item['retirementproducer'].strip() if item['retirementproducer'] else '',
                "retirementproducersfielduri":rail.result('get_required_customfield_uris')['cf_retirementproducers'],
                "retirementproducersoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_retirementproducers'),'displayText',item['retirementproducer'].strip() if
                    item['retirementproducer'] else '','uri',''),
                "LocktonRetirementServicingOffice": item['locktonretirementservicingoffice'].strip() if item['locktonretirementservicingoffice'] else '',
                "locktonretirementservicingofficefielduri":rail.result('get_required_customfield_uris')['cf_locktonretirementservicingoffice'],
                "locktonretirementservicingofficeoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_locktonretirementservicingoffice'),'displayText',item['locktonretirementservicingoffice'].strip() if
                    item['locktonretirementservicingoffice'] else '','uri',''),
                "LocktonBenefitsServicingOffice": item['locktonbenefitsservicingoffice'].strip() if item['locktonbenefitsservicingoffice'] else '',
                "locktonbenefitsservicingofficefielduri":rail.result('get_required_customfield_uris')['cf_locktonbenefitsservicingoffice'],
                "locktonbenefitsservicingofficeoptionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_dropdown_options_locktonbenefitsservicingoffice'),'displayText',item['locktonbenefitsservicingoffice'].strip() if
                    item['locktonbenefitsservicingoffice'] else '','uri',''),
                "ParentCompany": item['parentcompany'],
                "parentcompanyfielduri": rail.result('get_required_customfield_uris')['cf_parentcompany'],
                "ParentDnBNumber": item['parentdnbnumber'] or item['clientdnbnumber'],
                "parentdnbnumberfielduri": rail.result('get_required_customfield_uris')['cf_parentdnbnumber'],
                "ClientDnBNumber": item['clientdnbnumber'],
                "clientdnbnumberfielduri": rail.result('get_required_customfield_uris')['cf_clientdnbnumber'],
                "ClientDnBName": item['clientdnbname'],
                "clientdnbnamefielduri": rail.result('get_required_customfield_uris')['cf_clientdnbname'],
                "results": rail.find_first_by_attr_and_get_attr(rail.load_all_records(rail.result(
                    'create_collection_allclients')),'code',item['locktonmasterID'],'uri',''),
                "logslookuptable": rail.result('create_lockton_clientimport_logs_lookuptable'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        search_logs_in_lookuptable=rail.FilterLogEntriesOperator(
            task_id='search_logs_in_lookuptable',
            log="{{result('create_lockton_clientimport_logs_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        if_logs_present=rail.IfOperator(
            task_id='if_logs_present',
            test='''{{ result('search_logs_in_lookuptable','length') > 0 }}''',
            yes_task="compose_csv_logs",
            no_task="archive_reference_file",
        )

        compose_csv_logs=rail.WriteCSVFileOperator(
            task_id='compose_csv_logs',
            source="{{ result('search_logs_in_lookuptable') }}",
            header=['JobID',
                    'LocktonMasterID',
                    'Client Name',
                    'Status',
                    'Details'],
            row= [
                "{{ item.properties.jobid }}",
                "{{ item.properties.locktonmasterid }}",
                "{{ item.properties.clientname }}",
                "{{ item.properties.status }}",
                "{{ item.properties.details }}"
            ],
        )

        get_logfile_name=rail.PythonOperator(
            task_id='get_logfile_name',
            python_callable= lambda:  "LocktonClientImportLogs_" + rail.result('log_start_time')['format2'] + ".csv"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv_logs')}}",
            output_file_name="{{ result('get_logfile_name')}}",
            expires_in_seconds=7*24*60*60,
        )

        send_success_mail=rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''LocktonCompanies | Client Import logs {{result('log_start_time').format1}}''',
            html_content= '''templates/success_mail.html''',
        )

        archive_reference_file=rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            sftp_conn_id=config.sftp_internal_conn_id,
            existing_filename=config.reference_filepath + 'reference_file.csv',
            new_filename=config.archive_filepath + 'referencefile_' + '{{current_time_in_specified_tz(fmt="%d%m%Y%H%M%S")}}' + '.csv'
        )

        upload_new_reference_file=rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            sftp_conn_id=config.sftp_internal_conn_id,
            content="{{result('load_csv_input_file')}}",
            remote_filepath=config.reference_filepath + 'reference_file.csv'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        list_files_in_input_folder >> log_start_time >> if_no_files_found
        if_no_files_found >> rail.Label('Yes')  >> send_mail_no_inputfile_found >> log_to_sumo
        if_no_files_found >> rail.Label(
            'No') >> get_clients_and_customfields_report_details >> get_inputfile_name  >> run_clientsandcustomfields_report >> get_allclients_report_details
        get_allclients_report_details >> run_allclients_report >> load_csv_from_allclients_report >> create_collection_allclients >> if_inputfile_ends_with_csv
        if_inputfile_ends_with_csv >> rail.Label('Yes')  >> download_input_file >> delete_input_file_for_archiving >> upload_input_file_for_archiving
        upload_input_file_for_archiving >> load_csv_input_file >> create_collection_input_file
        create_collection_input_file >> create_lockton_clientimport_logs_lookuptable >> get_required_customfield_uris
        get_required_customfield_uris >> get_dropdown_options_clientprospectnonclient >> get_dropdown_options_locktonbenefitsservicingoffice
        get_dropdown_options_locktonbenefitsservicingoffice >> get_dropdown_options_retirementproducers >> get_dropdown_options_ebproducer
        get_dropdown_options_ebproducer >> get_dropdown_options_pncproducer >> get_dropdown_options_locktonretirementservicingoffice
        get_dropdown_options_locktonretirementservicingoffice >> get_dropdown_options_locktonpncservicingoffice >> query_clients_tobe_enabled
        query_clients_tobe_enabled >> trigger_child_dag_enable_clients
        trigger_child_dag_enable_clients >> wait_for_child_enable_clients >> download_reference_file >> load_csv_reference_file
        load_csv_reference_file >> create_collection_reference_file >> query_deltavalues >> query_clients_to_be_updated
        query_clients_to_be_updated >> trigger_child_update_client >> query_eligible_clients_in_instance
        query_eligible_clients_in_instance >> query_clients_tobe_disabled >> trigger_child_disable_clients
        trigger_child_disable_clients >> wait_for_child_disable_clients >> query_new_clients >> trigger_child_add_new_clients >> \
        search_logs_in_lookuptable >> if_logs_present
        if_logs_present >> rail.Label('Yes') >> compose_csv_logs >> get_logfile_name >> generate_download_link >> send_success_mail
        send_success_mail >> archive_reference_file >> upload_new_reference_file >> log_to_sumo
        if_logs_present >> rail.Label('No') >> archive_reference_file
        if_inputfile_ends_with_csv >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
