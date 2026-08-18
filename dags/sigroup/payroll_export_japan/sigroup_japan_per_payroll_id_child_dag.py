import re
from datetime import timedelta, datetime
from sigroup.payroll_export_japan.mappers.sigroup_payelements_mapper import sigroup_payelements_mapper
from sigroup.payroll_export_japan.mappers.sigroup_japan_calendar_mapper import sigroup_japan_calendar
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sigroup_payroll_export_japan_per_payroll_id_child_{config.instance}',
        description=f'SiGroup - Japan_per_payroll_id_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_payrollid_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_payrollid_not_present',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_payrollid_not_present=rail.IfOperator(
            task_id='if_payrollid_not_present',
            test='''{{ dag_run.conf.payrollid | is_falsy }}''',
            yes_task="finish",
            no_task="create_payload_per_payrollid",
        )

        create_payload_per_payrollid = rail.QueryCollectionOperator(
            task_id = 'create_payload_per_payrollid',
            name = 'payloadperpayrollid',
            query="""SELECT * FROM structureddata WHERE structureddata.cloudpaypaycode = '{{dag_run.conf.payrollid}}'"""
        )

        if_enduserdata_present = rail.IfOperator(
            task_id = 'if_enduserdata_present',
            test=lambda dag_run: bool( dag_run.conf['isenduserdatapresent'] and dag_run.conf['isenduserdatapresent']!= 'None'),
            yes_task='get_enduserdata_collection',
            no_task='get_alluserswithhourlypayrollrate_collection'
        )

        get_enduserdata_collection = rail.QueryCollectionOperator(
            task_id = 'get_enduserdata_collection',
            query="""SELECT * FROM enduserdata"""
        )

        get_alluserswithhourlypayrollrate_collection = rail.QueryCollectionOperator(
            task_id = 'get_alluserswithhourlypayrollrate_collection',
            query="""SELECT * FROM alluserwithhourlypayrollrate"""
        )

        load_users_with_hourlypayrollrate = rail.PythonOperator(
            task_id = 'load_users_with_hourlypayrollrate',
            python_callable= lambda: rail.load_all_records(rail.result('get_alluserswithhourlypayrollrate_collection'))
        )

        get_valid_mapper_entries=rail.PythonOperator(
            task_id='get_valid_mapper_entries',
            python_callable= lambda: (list(filter(lambda x: x["exported"] == "Yes" , sigroup_payelements_mapper)))
        )

        compose_csv_for_data_with_paycode_type = rail.WriteCSVFileOperator(
            task_id = 'compose_csv_for_data_with_paycode_type',
            source="{{result('create_payload_per_payrollid')}}",
            header=['employeeid','entrydate','cloudpaypaycode','paycodecode','paycodehours','paycodepay','paycodetype'],
            row=lambda item:[
                item['employeeid'],
                item['entrydate'],
                item['cloudpaypaycode'],
                item['paycodecode'],
                item['paycodehours'],
                item['paycodepay'],
                rail.find_first_by_attr_and_get_attr(rail.result('get_valid_mapper_entries'),'identifier',item['paycodecode'],'value','')
            ]
        )

        create_validatedpayloadperpayrollid_collection = rail.CreateCollectionOperator(
            task_id = 'create_validatedpayloadperpayrollid_collection',
            source="{{result('compose_csv_for_data_with_paycode_type')}}",
            name='validatedpayloadperpayrollid',
            columns={
                'employeeid':'employeeid',
                'entrydate':'entrydate',
                'cloudpaypaycode':'cloudpaypaycode',
                'paycodecode':'paycodecode',
                'paycodehours':'paycodehours',
                'paycodepay':'paycodepay',
                'paycodetype':'paycodetype'
            }
        )

        get_all_pay_codes_paycodewith_multipliers=rail.RepliconServiceOperator(
            task_id='get_all_pay_codes_paycodewith_multipliers',
            endpoint="/services/PayCodeService1.svc/GetAllPayCodes",
        )

        create_employeedata_list=rail.SetVariableOperator(
            task_id='create_employeedata_list',
            append=False,
            name='employeedata',
            value=[]
        )

        query_distinct_employee_ids_and_cloudpayids=rail.QueryCollectionOperator(
            task_id='query_distinct_employee_ids_and_cloudpayids',
            query="""SELECT DISTINCT  payloadperpayrollid.employeeid,  payloadperpayrollid.cloudpaypaycode FROM  payloadperpayrollid """,
        )

        foreach_query_distinct_employeeid_cloudpayid=rail.ForEachOperator(
            task_id='foreach_query_distinct_employeeid_cloudpayid',
            items="{{result('query_distinct_employee_ids_and_cloudpayids')}}",
            start_task = 'create_temppay_list_variable',
            end_task = 'foreach_query_distinct_employeeid_cloudpayid_end'
        )

        create_temppay_list_variable=rail.SetVariableOperator(
            task_id='create_temppay_list_variable',
            append=False,
            name='temppay',
            value=[]
        )

        create_timebasedpay_list_variable=rail.SetVariableOperator(
            task_id='create_timebasedpay_list_variable',
            append=False,
            name='timebasedpay',
            value=[]
        )

        query_final_data_foruser_for_timebasedpay_elements=rail.QueryCollectionOperator(
            task_id='query_final_data_foruser_for_timebasedpay_elements',
            query="""SELECT * FROM  validatedpayloadperpayrollid WHERE
                validatedpayloadperpayrollid.employeeid='{{ result('foreach_query_distinct_employeeid_cloudpayid').employeeid }}' AND
                validatedpayloadperpayrollid.paycodetype = 'Time Based Pay Elements'""",
        )

        if_finaldata_for_timebasedpay_users_present = rail.IfOperator(
            task_id = 'if_finaldata_for_timebasedpay_users_present',
            test="{{result('query_final_data_foruser_for_timebasedpay_elements','length') > 0}}",
            yes_task='insert_to_timebasedpay_list',
            no_task='query_final_data_foruser_for_temporarypay_elements'
        )

        def get_base_rate(employeeid):
            return rail.find_first_by_attr_and_get_attr(rail.result('load_users_with_hourlypayrollrate'),'employeeid',employeeid,'currenthourlypayroll','')

        def get_timebasedpay_data():
            load_final_data = rail.load_all_records(rail.result('query_final_data_foruser_for_timebasedpay_elements'))
            return [{
                "operationtype": "NONE",
                "rateid": data['paycodecode'],
                "type": "PAYMENT",
                "format": "HOURS_DECIMAL",
                "value": data['paycodehours'],
                "baserate": get_base_rate(data['employeeid']),
                "effectivedate": data['entrydate'],
                "datetype": "DATE_OF",
            } for data in load_final_data]

        insert_to_timebasedpay_list = rail.SetVariableOperator(
            task_id = 'insert_to_timebasedpay_list',
            name="{{result('create_timebasedpay_list_variable').name}}",
            append=True,
            value=get_timebasedpay_data
        )

        query_final_data_foruser_for_temporarypay_elements = rail.QueryCollectionOperator(
            task_id = 'query_final_data_foruser_for_temporarypay_elements',
            query="""SELECT * FROM  validatedpayloadperpayrollid WHERE
                validatedpayloadperpayrollid.employeeid='{{ result('foreach_query_distinct_employeeid_cloudpayid').employeeid }}' AND
                validatedpayloadperpayrollid.paycodetype = 'Temporary Pay Element'"""
        )

        if_finaldata_for_temporarypay_users_present = rail.IfOperator(
            task_id = 'if_finaldata_for_temporarypay_users_present',
            test="{{result('query_final_data_foruser_for_temporarypay_elements','length') > 0}}",
            yes_task='insert_to_temppay_list',
            no_task='if_enduserdata_present_and_has_rows'
        )


        def get_temppay_data():
            final_data = rail.load_all_records(rail.result('query_final_data_foruser_for_temporarypay_elements'))
            return [{
                "operationtype": "NONE",
                "paycode": data['paycodecode'],
                "value": data['paycodepay'],
                "effectivedate": data['entrydate']
            } for data in final_data]

        insert_to_temppay_list = rail.SetVariableOperator(
            task_id = 'insert_to_temppay_list',
            name="{{result('create_temppay_list_variable').name}}",
            append=True,
            value=get_temppay_data
        )

        if_enduserdata_present_and_has_rows=rail.IfOperator(
            task_id='if_enduserdata_present_and_has_rows',
            test='''{{ result('get_enduserdata_collection') | is_truthy and result('get_enduserdata_collection','length') > 0 }}''',
            yes_task="query_enduserdata_for_user",
            no_task="if_data_to_export_present",
        )

        query_enduserdata_for_user=rail.QueryCollectionOperator(
            task_id='query_enduserdata_for_user',
            query="""SELECT * FROM  enduserdata WHERE  enduserdata.employeeid='{{ result('foreach_query_distinct_employeeid_cloudpayid').employeeid }}' AND
                enduserdata.timeofftypedescription LIKE 'Export Balance on Termination%' """,
        )

        if_enduserdata_for_user_present = rail.IfOperator(
            task_id = 'if_enduserdata_for_user_present',
            test = "{{result('query_enduserdata_for_user','length') > 0}}",
            yes_task='insert_to_time_based_pay_list',
            no_task='if_data_to_export_present'
        )

        def get_timebasedpay_data_add():
            load_final_data = rail.load_all_records(rail.result('query_enduserdata_for_user'))
            return [{
                "operationtype": "ADD",
                "rateid": ((data['timeofftypedescription'].split("|"))[-1]).strip(),
                "type": "PAYMENT",
                "format": 'HOURS_DECIMAL' if data['units'] == 'Hours' else 'DAYS_DECIMAL',
                "value": data['timeoffbalance'],
                "baserate": '',
                "effectivedate": data['userenddate'],
                "datetype": "WEEK_COMMENCING",
            } for data in load_final_data]

        insert_to_time_based_pay_list = rail.SetVariableOperator(
            task_id = 'insert_to_time_based_pay_list',
            name="{{result('create_timebasedpay_list_variable').name}}",
            append=True,
            value=get_timebasedpay_data_add
        )

        if_data_to_export_present = rail.IfOperator(
            task_id = 'if_data_to_export_present',
            test=lambda: bool(rail.get_dag_run_var('temppay') or rail.get_dag_run_var('timebasedpay')),
            yes_task='insert_to_employeedata_list',
            no_task='foreach_query_distinct_employeeid_cloudpayid_end'
        )

        insert_to_employeedata_list=rail.SetVariableOperator(
            task_id='insert_to_employeedata_list',
            append=True,
            name='{{ result("create_employeedata_list").name }}',
            value=lambda: {
                "EmployeeNumber": rail.result('foreach_query_distinct_employeeid_cloudpayid')['employeeid'],
                "TemporaryPayElement": [item for element in rail.get_dag_run_var('temppay') for item in element ],
                "TimeBasedPayElement": [item for element in rail.get_dag_run_var('timebasedpay') for item in element ]
            }
        )

        foreach_query_distinct_employeeid_cloudpayid_end=rail.EmptyOperator(
            task_id='foreach_query_distinct_employeeid_cloudpayid_end',
        )

        if_alluserswithourlypayrollrate_and_enduserdata_present=rail.IfOperator(
            task_id='if_alluserswithourlypayrollrate_and_enduserdata_present',
            test='''{{ result('get_alluserswithhourlypayrollrate_collection','length') > 0  and result('get_enduserdata_collection') | is_truthy}}''',
            yes_task="query_unique_employees_from_enduserdata_not_in_payloadperpayrollid",
            no_task="get_entries_from_calendar_mapper",
        )

        query_unique_employees_from_enduserdata_not_in_payloadperpayrollid=rail.QueryCollectionOperator(
            task_id='query_unique_employees_from_enduserdata_not_in_payloadperpayrollid',
            query="""SELECT DISTINCT  enduserdata.employeeid FROM  enduserdata WHERE
                enduserdata.employeeid NOT IN (SELECT DISTINCT  payloadperpayrollid.employeeid FROM  payloadperpayrollid) """,
        )

        foreach_unique_employeeid=rail.ForEachOperator(
            task_id='foreach_unique_employeeid',
            items="{{ result('query_unique_employees_from_enduserdata_not_in_payloadperpayrollid') }}",
            start_task = 'query_enduserdata_per_employeeid',
            end_task = 'foreach_enduser_data_end'
        )

        query_enduserdata_per_employeeid=rail.QueryCollectionOperator(
            task_id='query_enduserdata_per_employeeid',
            query="""SELECT * FROM  enduserdata WHERE  enduserdata.employeeid='{{ result('foreach_unique_employeeid').employeeid }}' AND
                enduserdata.timeofftypedescription LIKE 'Export Balance on Termination%'""",
        )

        create_temporarypayelement_list=rail.SetVariableOperator(
            task_id='create_temporarypayelement_list',
            append=False,
            name='temporarypayelement',
            value=[]
        )

        create_timebasedpayelement_list=rail.SetVariableOperator(
            task_id='create_timebasedpayelement_list',
            append=False,
            name='timebasedpayelement',
            value=[]
        )

        if_enduserdata_per_employeeid_present = rail.IfOperator(
            task_id = 'if_enduserdata_per_employeeid_present',
            test="{{result('query_enduserdata_per_employeeid','length') > 0}}",
            yes_task='add_to_timebasedpay_list',
            no_task='add_to_employeedata_list'
        )

        def get_add_timebasedpay_data():
            load_final_data = rail.load_all_records(rail.result('query_enduserdata_per_employeeid'))
            return [{
                "operationtype": "ADD",
                "rateid": ((data['timeofftypedescription'].split("|"))[-1]).strip(),
                "type": "PAYMENT",
                "format": "HOURS_DECIMAL" if data['units'] == 'Hours' else "DAYS_DECIMAL",
                "value": data['timeoffbalance'],
                "baserate": '',
                "effectivedate": data['userenddate'],
                "datetype": "WEEK_COMMENCING"
            } for data in load_final_data]

        add_to_timebasedpay_list=rail.SetVariableOperator(
            task_id='add_to_timebasedpay_list',
            append=True,
            name='{{ result("create_timebasedpayelement_list").name }}',
            value=get_add_timebasedpay_data
        )

        add_to_employeedata_list=rail.SetVariableOperator(
            task_id='add_to_employeedata_list',
            append=True,
            name='{{ result("create_employeedata_list").name }}',
            value = lambda: {
                "EmployeeNumber": rail.result('foreach_unique_employeeid')['employeeid'],
                "TemporaryPayElement": [item for element in rail.get_dag_run_var('temporarypayelement') for item in element ],
                "TimeBasedPayElement": [item for element in rail.get_dag_run_var('timebasedpayelement') for item in element ]
            }
        )

        foreach_enduser_data_end=rail.EmptyOperator(
            task_id='foreach_enduser_data_end',
        )

        get_entries_from_calendar_mapper=rail.PythonOperator(
            task_id='get_entries_from_calendar_mapper',
            python_callable= lambda:  (list(filter(lambda x: x["export"] == "Yes" and x["fileformat"] == "Japan" and x["repliconextractdate"] == datetime.now().strftime("%d-%m-%Y"), sigroup_japan_calendar)))
        )

        get_file_name=rail.PythonOperator(
            task_id='get_file_name',
            python_callable= lambda:  "SIGTT" + "-" + rail.result('get_entries_from_calendar_mapper')[0]['filenamecounter'] + "-" +
                                "PROD-CPM-" + datetime.now().strftime('%Y%m%d') + "-" + datetime.now().strftime('%H%M%S') + "-" + "PD21.xml"
        )

        get_main_payload_fields=rail.PythonOperator(
            task_id='get_main_payload_fields',
            python_callable= lambda dag_run: {
                "extractfromdate": datetime.strptime(dag_run.conf['startdate'],'%d-%m-%Y').strftime("%Y-%m-%dT%H:%M:%S"),
                "extracttodate": datetime.strptime(dag_run.conf['enddate'],'%d-%m-%Y').strftime("%Y-%m-%dT%H:%M:%S"),
                "filename": rail.result('get_file_name'),
                "creationdate": datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            }
        )

        get_employeedata_list_value = rail.GetVariableOperator(
            task_id = 'get_employeedata_list_value',
            name='employeedata'
        )

        create_employeedata_in_xml = rail.RenderTemplateOperator(
            task_id = 'create_employeedata_in_xml',
            target='result',
            template_file = '''employeedata.xml''',
            dataset="{{result('get_employeedata_list_value').value | to_json}}"
        )

        def refactor(xml_string):
            #pylint: disable= line-too-long
            xml_string = xml_string.replace('<?xml version="1.0" encoding="UTF-8"?>\n<active-support-hash-with-indifferent-accesses type="array">\n<active-support-hash-with-indifferent-access>\n','')
            xml_string = re.sub(r'<(\w+[\w-]*)(\s+[^>]*)?>',r'<pd:\1\2>',xml_string)
            xml_string = re.sub(r'</(\w+[\w-]*)>',r'</pd:\1>',xml_string)
            return xml_string.replace('type="array"','').replace('</pd:active-support-hash-with-indifferent-access>\n','').replace('\n</pd:active-support-hash-with-indifferent-accesses>','').replace('<pd:active-support-hash-with-indifferent-access>\n','').replace('<pd:TemporaryPayElement >\n','').replace(
                '<pd:TemporaryPayElement />\n','').replace('<TimeBasedPayElement type="array">\n','').replace('<TemporaryPayElement type="array">\n','').replace('<pd:TimeBasedPayElement >\n','').replace('<pd:TimeBasedPayElement />\n','').replace(
                '<pd:TemporaryPayElement>\n</pd:TemporaryPayElement>\n</pd:TemporaryPayElement>\n','').replace('</pd:TemporaryPayElement>\n</pd:TemporaryPayElement>\n','</pd:TemporaryPayElement>\n').replace(
                '</pd:TimeBasedPayElement>\n</pd:TimeBasedPayElement>\n','</pd:TimeBasedPayElement>\n').replace('<pd:TimeBasedPayElement>\n</pd:TimeBasedPayElement>\n</pd:TimeBasedPayElement>\n','')

        filter_employeedata_xml = rail.PythonOperator(
            task_id = 'filter_employeedata_xml',
            python_callable= lambda: refactor(rail.result('create_employeedata_in_xml'))
        )

        create_main_payload_document=rail.RenderTemplateOperator(
            task_id='create_main_payload_document',
            target='result',
            template_file = '''payload.xml'''
        )

        def remove_empty_tags():
            pattern1 = r"\s*[^\s]+></[^\s]+>"
            pattern2 = r"\s*<\w+[^0-9a-zA-Z]+\w+>[\r\n]<\/\w+[^0-9a-zA-Z]+\w+>"
            input_string = rail.result('create_main_payload_document')
            output_string = re.sub(pattern1,'',input_string)
            output_string = re.sub(pattern2,'',output_string)
            return output_string

        payload_after_removing_empty_tags=rail.PythonOperator(
            task_id='payload_after_removing_empty_tags',
            python_callable= remove_empty_tags
        )

        log_final_output=rail.PythonOperator(
            task_id='log_final_output',
            python_callable= lambda: rail.result('payload_after_removing_empty_tags').replace(
                                '<pd:TemporaryPayElement>\n<pd:TimeBasedPayElement>\n','<pd:TimeBasedPayElement>\n').replace(
                                '<pd:TimeBasedPayElement>\n<pd:TemporaryPayElement>\n','<pd:TemporaryPayElement>\n').replace(
                                '</pd:EmployeeNumber>\n</pd:TemporaryPayElement>\n','</pd:EmployeeNumber>\n')
        )

        upload_final_output_to_sftp=rail.SFTPUploadFileOperator(
            task_id='upload_final_output_to_sftp',
            content='''{{ result('log_final_output') }}''',
            remote_filepath= config.log_upload_path + '''{{ result('get_file_name') }}''',
            sftp_conn_id= config.secondary_sftp_conn_id,
        )

        encrypt_the_file=rail.PGPEncryptionOperator(
            task_id='encrypt_the_file',
            source="{{result('log_final_output')}}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_file=rail.SFTPUploadFileOperator(
            task_id='upload_encrypted_file',
            content="{{result('encrypt_the_file')}}",
            remote_filepath= config.file_upload_path + "{{result('get_file_name') }}.pgp",
        )

        upload_encrypted_file_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_encrypted_file_secondary_sftp',
            content="{{result('encrypt_the_file')}}",
            remote_filepath= config.log_upload_path + "{{result('get_file_name') }}.pgp",
            sftp_conn_id= config.secondary_sftp_conn_id,
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> if_payrollid_not_present
        if_payrollid_not_present >> rail.Label('Yes')  >> finish
        if_payrollid_not_present >> rail.Label('No') >> create_payload_per_payrollid >> if_enduserdata_present
        if_enduserdata_present >> rail.Label('Yes') >> get_enduserdata_collection >> get_alluserswithhourlypayrollrate_collection
        if_enduserdata_present >> rail.Label('No') >> get_alluserswithhourlypayrollrate_collection >> load_users_with_hourlypayrollrate
        load_users_with_hourlypayrollrate >> get_valid_mapper_entries
        get_valid_mapper_entries >> compose_csv_for_data_with_paycode_type >> create_validatedpayloadperpayrollid_collection
        create_validatedpayloadperpayrollid_collection >> get_all_pay_codes_paycodewith_multipliers >> create_employeedata_list
        create_employeedata_list >> query_distinct_employee_ids_and_cloudpayids
        query_distinct_employee_ids_and_cloudpayids >> foreach_query_distinct_employeeid_cloudpayid >> create_temppay_list_variable
        create_temppay_list_variable >> create_timebasedpay_list_variable
        create_timebasedpay_list_variable >> query_final_data_foruser_for_timebasedpay_elements >> if_finaldata_for_timebasedpay_users_present
        if_finaldata_for_timebasedpay_users_present >> rail.Label('Yes') >> insert_to_timebasedpay_list >> query_final_data_foruser_for_temporarypay_elements
        if_finaldata_for_timebasedpay_users_present >> rail.Label(
            'No') >> query_final_data_foruser_for_temporarypay_elements >> if_finaldata_for_temporarypay_users_present
        if_finaldata_for_temporarypay_users_present >> rail.Label('Yes') >> insert_to_temppay_list >> if_enduserdata_present_and_has_rows
        if_finaldata_for_temporarypay_users_present >> rail.Label('No') >> if_enduserdata_present_and_has_rows
        if_enduserdata_present_and_has_rows >> rail.Label('Yes') >> query_enduserdata_for_user >> if_enduserdata_for_user_present
        if_enduserdata_present_and_has_rows >> rail.Label('No') >> if_data_to_export_present
        if_enduserdata_for_user_present >> rail.Label(
            'Yes') >> insert_to_time_based_pay_list >> if_data_to_export_present
        if_data_to_export_present >> rail.Label('Yes') >> insert_to_employeedata_list >> foreach_query_distinct_employeeid_cloudpayid_end
        if_data_to_export_present >> rail.Label('No') >> foreach_query_distinct_employeeid_cloudpayid_end
        if_enduserdata_for_user_present >> rail.Label('No') >> if_data_to_export_present
        foreach_query_distinct_employeeid_cloudpayid >> foreach_query_distinct_employeeid_cloudpayid_end
        foreach_query_distinct_employeeid_cloudpayid_end >> if_alluserswithourlypayrollrate_and_enduserdata_present
        if_alluserswithourlypayrollrate_and_enduserdata_present >> rail.Label(
            'Yes') >> query_unique_employees_from_enduserdata_not_in_payloadperpayrollid >> foreach_unique_employeeid
        foreach_unique_employeeid >> query_enduserdata_per_employeeid >> create_temporarypayelement_list >> create_timebasedpayelement_list
        create_timebasedpayelement_list >> if_enduserdata_per_employeeid_present >> rail.Label('Yes') >> add_to_timebasedpay_list >> add_to_employeedata_list
        if_enduserdata_per_employeeid_present >> rail.Label('No') >> add_to_employeedata_list >> foreach_enduser_data_end
        foreach_unique_employeeid >> foreach_enduser_data_end >> get_entries_from_calendar_mapper
        if_alluserswithourlypayrollrate_and_enduserdata_present >> rail.Label('No') >> get_entries_from_calendar_mapper >> get_file_name
        get_file_name >> get_main_payload_fields >> get_employeedata_list_value >> create_employeedata_in_xml >> filter_employeedata_xml
        filter_employeedata_xml >> create_main_payload_document >> payload_after_removing_empty_tags >> log_final_output >> upload_final_output_to_sftp
        upload_final_output_to_sftp >> encrypt_the_file >> upload_encrypted_file >> upload_encrypted_file_secondary_sftp >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
