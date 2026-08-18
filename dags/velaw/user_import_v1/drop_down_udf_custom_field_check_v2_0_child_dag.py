
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.drop_down_udf_custom_field_check_dag_id,
        description=f'VelawG3 Drop Down UDF Custom field check V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='parse_csv_raw_data_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='parse_csv_raw_data_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        parse_csv_raw_data_4 = rail.LoadCSVFileOperator(
            task_id='parse_csv_raw_data_4',
            document="{{ dag_run.conf.filepath }}",
            encoding="ISO-8859-1"
        )

        def get_csv_rows(item):
            row_data = [
                item['FIRST_NAME'],
                item['LAST_NAME'],
                item['EMAIL'],
                item['EMPLOYEE_ID'],
                item['START_DATE'],
                item['END_DATE'],
                item['JOB_CODE'],
                item['JOB_TITLE'].strip(
                ) if item['JOB_TITLE'] else null,
                item['FLSA_STATUS'].strip(
                ) if item['FLSA_STATUS'] else null,
                item['ASSIGNMENT_CATEGORY'].strip(
                ) if item['ASSIGNMENT_CATEGORY'] else null,
                item['COUNTRY_ISO_CODE'].strip(
                ) if item['COUNTRY_ISO_CODE'] else null,
                item['PERSON_TYPE'],
                item['LEGAL_EMPLOYER'].strip(
                ) if item['LEGAL_EMPLOYER'] else null,
                item['LOGIN_NAME'].strip(
                ) if item['LOGIN_NAME'] else null,
                item['SUPERVISOR_LOGIN_NAME'],
                item['IS_LOGIN_ENABLED'],
                item['DEPARTMENT_NAME'].strip(
                ) if item['DEPARTMENT_NAME'] else null,
                item['DEPARTMENT_CODE'].strip(
                ) if item['DEPARTMENT_CODE'] else null,
                item['EMPLOYEE_TYPE'].strip() if item['EMPLOYEE_TYPE'] else null,
                item['LOCATION'].strip() if item['LOCATION'] else null,
                item['JOB_FAMILIES'].strip() if item['JOB_FAMILIES'] else null,
                item['PAY_TYPE'].strip() if item['PAY_TYPE'] else null,
                item['PAY_RATES_AMOUNT'],
                item['PAY_RATES_CURRENCY'],
                item['DEFAULT_BILLING_RATE_AMOUNT'],
                item['DEFAULT_BILLING_RATE_CURRENCY'],
                item['HOURLY_COST_AMOUNT'],
                item['HOURLY_COST_CURRENCY']
            ]
            return row_data

        create_csv_lines_formatted_data_5 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_formatted_data_5',
            source="{{ result('parse_csv_raw_data_4') }}",
            header=['firstname',
                    'lastname',
                    'email',
                    'employeeid',
                    'startdate',
                    'enddate',
                    'jobcode',
                    'jobtitle',
                    'flsastatus',
                    'assignmentcategory',
                    'countryisocode',
                    'persontype',
                    'legalemployer',
                    'loginname',
                    'supervisorloginname',
                    'isloginenabled',
                    'departmentname',
                    'departmentcode',
                    'employeetype',
                    'location',
                    'jobfamilies',
                    'paytype',
                    'payratesamount',
                    'payratescurrency',
                    'defaultbillingrateamount',
                    'defaultbillingratecurrency',
                    'hourlycostamount',
                    'hourlycostcurrency'],
            row=get_csv_rows
        )

        load_csv_create_list_from_csv_6 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_6",
            document="{{ result('create_csv_lines_formatted_data_5') }}",
        )

        create_collection_create_list_from_csv_6 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_6',
            source="{{ result('load_csv_create_list_from_csv_6') }}",
            name="rawinputdata"
        )

        query_list_getallthedata_7 = rail.QueryCollectionOperator(
            task_id='query_list_getallthedata_7',
            query="""SELECT * FROM rawinputdata"""
        )

        query_list_getallthedistinct_job_code_8 = rail.QueryCollectionOperator(
            task_id='query_list_getallthedistinct_job_code_8',
            query="""SELECT DISTINCT jobcode FROM rawinputdata WHERE NULLIF(jobcode,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="jobcode"
        )

        query_list_getallthe_job_title_10 = rail.QueryCollectionOperator(
            task_id='query_list_getallthe_job_title_10',
            query="""SELECT DISTINCT jobtitle FROM rawinputdata WHERE NULLIF(jobtitle,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="jobtitle"
        )

        query_list_getallthe_f_l_s_a_status_12 = rail.QueryCollectionOperator(
            task_id='query_list_getallthe_f_l_s_a_status_12',
            query="""SELECT DISTINCT flsastatus FROM rawinputdata WHERE NULLIF(flsastatus,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="flsastatus"
        )

        query_list_getallthe_assignment_category_14 = rail.QueryCollectionOperator(
            task_id='query_list_getallthe_assignment_category_14',
            query="""SELECT DISTINCT assignmentcategory FROM rawinputdata WHERE NULLIF(assignmentcategory,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="assignmentcategory"
        )

        query_list_getallthe_person_type_16 = rail.QueryCollectionOperator(
            task_id='query_list_getallthe_person_type_16',
            query="""SELECT DISTINCT persontype FROM rawinputdata WHERE NULLIF(persontype,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="persontype"
        )

        query_list_getallthe_legal_employer_18 = rail.QueryCollectionOperator(
            task_id='query_list_getallthe_legal_employer_18',
            query="""SELECT DISTINCT legalemployer FROM rawinputdata WHERE NULLIF(legalemployer,'') IS NOT NULL AND (countryisocode='US' OR countryisocode='GB')""",
            name="legalemployer",
        )

        get_all_custom_field_drop_down_optionsfor_job_code_21 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsfor_job_code_21',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.jobcodeudfuri }}"
            }
        )

        get_all_custom_field_drop_down_optionsfor_job_title_22 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsfor_job_title_22',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.jobtitleudfuri }}"
            }
        )

        get_all_custom_field_drop_down_options_for_f_l_s_a_status_23 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_for_f_l_s_a_status_23',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.flsastatusudfuri }}"
            }
        )

        get_all_custom_field_drop_down_options_for_assignment_category_24 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_for_assignment_category_24',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.assignmentcategoryudfuri }}"
            }
        )

        get_all_custom_field_drop_down_options_for_person_type_25 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_for_person_type_25',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.persontypeudfuri }}"
            }
        )

        get_all_custom_field_drop_down_options_for_legal_employer_26 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_for_legal_employer_26',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.legalemployerudfuri }}"
            }
        )

        create_list_30 = rail.CreateCollectionOperator(
            task_id='create_list_30',
            source="{{ result('get_all_custom_field_drop_down_optionsfor_job_code_21') | to_json }}",
            name="jobcodevalues",
        )

        query_list_newvaluestoaddfor_jobcode_31 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoaddfor_jobcode_31',
            query="""SELECT DISTINCT jobcode FROM jobcode WHERE LOWER(jobcode) NOT IN (SELECT DISTINCT LOWER(displayText) FROM jobcodevalues)""",
        )

        if_first_jobcode_present_32 = rail.IfOperator(
            task_id='if_first_jobcode_present_32',
            test='''{{ result('query_list_newvaluestoaddfor_jobcode_31', 'length') > 0 }}''',
            yes_task="final_customfielddropdown_option_uris_37",
            no_task="create_list_44",
        )

        def get_uri_list(jobtype, existing_options, new_options):
            final_list = []
            all_custom_field_drop_down_options = list(map(lambda item: {
                "target": {
                    "uri": item['uri'],
                    "name": item['displayText']
                },
                "name": item['displayText'],
                "isEnabled": item['isEnabled']
            }, rail.result(existing_options)))

            new_custom_field_drop_down_optionslist = list(map(lambda item: {
                "target": {
                    "uri": null,
                    "name": null
                },
                "name": item[jobtype].strip(),
                "isEnabled": "true"
            }, rail.load_all_records(rail.result(new_options))))

            if all_custom_field_drop_down_options:
                final_list.extend(all_custom_field_drop_down_options)
            if new_custom_field_drop_down_optionslist:
                final_list.extend(new_custom_field_drop_down_optionslist)

            name_set = set()

            # Filter out duplicates and store in a new list
            unique_data = []
            for item in final_list:
                # Convert the dictionary to a tuple and check if it's already in the set
                item_tuple = tuple(item.items())
                if item_tuple[1] not in name_set:
                    unique_data.append(item)
                    name_set.add(item_tuple[1])
            return unique_data

        final_customfielddropdown_option_uris_37 = rail.PythonOperator(
            task_id="final_customfielddropdown_option_uris_37",
            python_callable=get_uri_list,
            op_args=['jobcode', 'get_all_custom_field_drop_down_optionsfor_job_code_21',
                     'query_list_newvaluestoaddfor_jobcode_31']
        )

        put_drop_down_optionsfor_job_code_38 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_job_code_38',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['jobcodeudfuri'],
                "customFieldDropDownOptionUris": rail.result('final_customfielddropdown_option_uris_37')
            }
        )

        create_list_44 = rail.CreateCollectionOperator(
            task_id='create_list_44',
            source="{{ result('get_all_custom_field_drop_down_optionsfor_job_title_22') | to_json }}",
            name="jobtitlevalues",
        )

        query_list_newvaluestoaddfor_jobtitle_45 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoaddfor_jobtitle_45',
            query="""SELECT DISTINCT jobtitle FROM jobtitle WHERE LOWER(jobtitle) NOT IN (SELECT DISTINCT LOWER(displayText) FROM jobtitlevalues)""",
        )

        if_first_jobtitle_present_46 = rail.IfOperator(
            task_id='if_first_jobtitle_present_46',
            test='''{{ result('query_list_newvaluestoaddfor_jobtitle_45', 'length') > 0 }}''',
            yes_task="final_customfielddropdown_option_jobtitle_uris",
            no_task="create_list_58",
        )

        final_customfielddropdown_option_jobtitle_uris = rail.PythonOperator(
            task_id="final_customfielddropdown_option_jobtitle_uris",
            python_callable=get_uri_list,
            op_args=['jobtitle', 'get_all_custom_field_drop_down_optionsfor_job_title_22',
                     'query_list_newvaluestoaddfor_jobtitle_45']
        )

        put_drop_down_optionsfor_job_title_52 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_job_title_52',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['jobtitleudfuri'],
                "customFieldDropDownOptionUris": rail.result('final_customfielddropdown_option_jobtitle_uris')
            }
        )

        create_list_58 = rail.CreateCollectionOperator(
            task_id='create_list_58',
            source="{{ result('get_all_custom_field_drop_down_options_for_f_l_s_a_status_23') | to_json }}",
            name="flsastatusvalues",
        )

        query_list_newvaluestoaddfor_f_l_s_a_status_59 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoaddfor_f_l_s_a_status_59',
            query="""SELECT DISTINCT flsastatus FROM flsastatus WHERE LOWER(flsastatus) NOT IN (SELECT DISTINCT LOWER(displayText) FROM flsastatusvalues)""",
        )

        if_first_flsastatus_present_60 = rail.IfOperator(
            task_id='if_first_flsastatus_present_60',
            test='''{{ result('query_list_newvaluestoaddfor_f_l_s_a_status_59', 'length') > 0 }}''',
            yes_task="final_customfielddropdown_option_flsastatus_uris",
            no_task="create_list_72",
        )

        final_customfielddropdown_option_flsastatus_uris = rail.PythonOperator(
            task_id="final_customfielddropdown_option_flsastatus_uris",
            python_callable=get_uri_list,
            op_args=['flsastatus', 'get_all_custom_field_drop_down_options_for_f_l_s_a_status_23',
                     'query_list_newvaluestoaddfor_f_l_s_a_status_59']
        )

        put_drop_down_optionsfor_f_l_s_a_status_66 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_f_l_s_a_status_66',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['flsastatusudfuri'],
                "customFieldDropDownOptionUris": rail.result('final_customfielddropdown_option_flsastatus_uris')
            }
        )

        create_list_72 = rail.CreateCollectionOperator(
            task_id='create_list_72',
            source="{{ result('get_all_custom_field_drop_down_options_for_assignment_category_24') | to_json }}",
            name="assignmentcategoryvalues",
        )

        query_list_newvaluestoaddfor_assignment_category_73 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoaddfor_assignment_category_73',
            query="""SELECT DISTINCT assignmentcategory FROM assignmentcategory WHERE LOWER(assignmentcategory) NOT IN (SELECT DISTINCT LOWER(displayText) FROM assignmentcategoryvalues)""",
        )

        if_first_assignmentcategory_present_74 = rail.IfOperator(
            task_id='if_first_assignmentcategory_present_74',
            test='''{{ result('query_list_newvaluestoaddfor_assignment_category_73', 'length') > 0 }}''',
            yes_task="final_customfielddropdown_option_assignmentcategory_uris",
            no_task="create_list_86",
        )

        final_customfielddropdown_option_assignmentcategory_uris = rail.PythonOperator(
            task_id="final_customfielddropdown_option_assignmentcategory_uris",
            python_callable=get_uri_list,
            op_args=['assignmentcategory', 'get_all_custom_field_drop_down_options_for_assignment_category_24',
                     'query_list_newvaluestoaddfor_assignment_category_73']
        )

        put_drop_down_optionsfor_assignment_category_80 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_assignment_category_80',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['assignmentcategoryudfuri'],
                "customFieldDropDownOptionUris":  rail.result('final_customfielddropdown_option_assignmentcategory_uris')
            }
        )

        create_list_86 = rail.CreateCollectionOperator(
            task_id='create_list_86',
            source="{{ result('get_all_custom_field_drop_down_options_for_person_type_25') | to_json }}",
            name="persontypevalues",
        )

        query_list_newvaluestoaddfor_person_types_87 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoaddfor_person_types_87',
            query="""SELECT DISTINCT persontype FROM persontype WHERE LOWER(persontype) NOT IN (SELECT DISTINCT LOWER(displayText) FROM persontypevalues)""",
        )

        if_first_persontype_present_88 = rail.IfOperator(
            task_id='if_first_persontype_present_88',
            test='''{{ result('query_list_newvaluestoaddfor_person_types_87', 'length') > 0 }}''',
            yes_task="final_customfielddropdown_option_persontype_uris",
            no_task="create_list_100",
        )

        final_customfielddropdown_option_persontype_uris = rail.PythonOperator(
            task_id="final_customfielddropdown_option_persontype_uris",
            python_callable=get_uri_list,
            op_args=['persontype', 'get_all_custom_field_drop_down_options_for_person_type_25',
                     'query_list_newvaluestoaddfor_person_types_87']
        )

        put_drop_down_optionsfor_persontype_94 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_persontype_94',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['persontypeudfuri'],
                "customFieldDropDownOptionUris": rail.result('final_customfielddropdown_option_persontype_uris')
            }
        )

        create_list_100 = rail.CreateCollectionOperator(
            task_id='create_list_100',
            source="{{ result('get_all_custom_field_drop_down_options_for_legal_employer_26') | to_json }}",
            name="legalemployervalues",
        )

        query_list_newvaluestoaddfor_legalemployer_101 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoaddfor_legalemployer_101',
            query="""SELECT DISTINCT legalemployer FROM legalemployer WHERE LOWER(legalemployer) NOT IN (SELECT DISTINCT LOWER(displayText) FROM legalemployervalues)""",
        )

        if_first_legalemployer_present_102 = rail.IfOperator(
            task_id='if_first_legalemployer_present_102',
            test='''{{ result('query_list_newvaluestoaddfor_legalemployer_101', 'length') > 0 }}''',
            yes_task="final_customfielddropdown_option_legalemployer_uris",
            no_task="log_to_sumo"
        )

        final_customfielddropdown_option_legalemployer_uris = rail.PythonOperator(
            task_id="final_customfielddropdown_option_legalemployer_uris",
            python_callable=get_uri_list,
            op_args=['legalemployer', 'get_all_custom_field_drop_down_options_for_legal_employer_26',
                     'query_list_newvaluestoaddfor_legalemployer_101']
        )

        put_drop_down_optionsfor_legal_employer_108 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_legal_employer_108',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: {
                "customFieldUri": dag_run.conf['legalemployerudfuri'],
                "customFieldDropDownOptionUris": rail.result('final_customfielddropdown_option_legalemployer_uris')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> parse_csv_raw_data_4 >> create_csv_lines_formatted_data_5 \
            >> load_csv_create_list_from_csv_6 >> create_collection_create_list_from_csv_6 \
            >> query_list_getallthedata_7 >> query_list_getallthedistinct_job_code_8 >> query_list_getallthe_job_title_10 \
            >> query_list_getallthe_f_l_s_a_status_12 >> query_list_getallthe_assignment_category_14 >> query_list_getallthe_person_type_16 \
            >> query_list_getallthe_legal_employer_18 >> get_all_custom_field_drop_down_optionsfor_job_code_21 \
            >> get_all_custom_field_drop_down_optionsfor_job_title_22 >> get_all_custom_field_drop_down_options_for_f_l_s_a_status_23 \
            >> get_all_custom_field_drop_down_options_for_assignment_category_24 >> get_all_custom_field_drop_down_options_for_person_type_25 \
            >> get_all_custom_field_drop_down_options_for_legal_employer_26 >> create_list_30 \
            >> query_list_newvaluestoaddfor_jobcode_31 >> if_first_jobcode_present_32
        if_first_jobcode_present_32 >> rail.Label(
            'Yes') >> final_customfielddropdown_option_uris_37 >> put_drop_down_optionsfor_job_code_38 >> create_list_44
        if_first_jobcode_present_32 >> rail.Label(
            'No') >> create_list_44 >> query_list_newvaluestoaddfor_jobtitle_45 >> if_first_jobtitle_present_46
        if_first_jobtitle_present_46 >> rail.Label(
            'Yes') >> final_customfielddropdown_option_jobtitle_uris >> put_drop_down_optionsfor_job_title_52 >> create_list_58
        if_first_jobtitle_present_46 >> rail.Label(
            'No') >> create_list_58 >> query_list_newvaluestoaddfor_f_l_s_a_status_59 >> if_first_flsastatus_present_60
        if_first_flsastatus_present_60 >> rail.Label(
            'Yes') >> final_customfielddropdown_option_flsastatus_uris >> put_drop_down_optionsfor_f_l_s_a_status_66 >> create_list_72
        if_first_flsastatus_present_60 >> rail.Label(
            'No') >> create_list_72 >> query_list_newvaluestoaddfor_assignment_category_73 >> if_first_assignmentcategory_present_74
        if_first_assignmentcategory_present_74 >> rail.Label(
            'Yes') >> final_customfielddropdown_option_assignmentcategory_uris >> put_drop_down_optionsfor_assignment_category_80 >> create_list_86
        if_first_assignmentcategory_present_74 >> rail.Label(
            'No') >> create_list_86 >> query_list_newvaluestoaddfor_person_types_87 >> if_first_persontype_present_88
        if_first_persontype_present_88 >> rail.Label(
            'Yes') >> final_customfielddropdown_option_persontype_uris >> put_drop_down_optionsfor_persontype_94 >> create_list_100
        if_first_persontype_present_88 >> rail.Label(
            'No') >> create_list_100 >> query_list_newvaluestoaddfor_legalemployer_101 >> if_first_legalemployer_present_102
        if_first_legalemployer_present_102 >> rail.Label(
            'Yes') >> final_customfielddropdown_option_legalemployer_uris >> put_drop_down_optionsfor_legal_employer_108 >> log_to_sumo
        if_first_legalemployer_present_102 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
