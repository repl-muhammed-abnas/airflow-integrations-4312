
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_portugal_suspend_assignment_category_custom_field_check_{config.instance}',
        description=f'GE_portugal Suspend Assignment Category Custom field check {config.instance}',
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
            no_task='create_list_5'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_list_5',
            end_task='catch_25',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_input_data(dag_run):
            return get_data_from_document(dag_run.conf['inputdata'])

        create_list_5 = rail.CreateCollectionOperator(
            task_id='create_list_5',
            source=get_input_data,
            name="inputdata",
            columns={
                "EmployeeFirstName": "EmployeeFirstName",
                "EmployeeLastName": "EmployeeLastName",
                "EmployeeEmailAddress": "EmployeeEmailAddress",
                "OHRID": "OHRID",
                "LegalEntityHireDate": "LegalEntityHireDate",
                "LegacyPayrollID": "LegacyPayrollID",
                "Job/PositionTitle": "Job_PositionTitle",
                "SupervisorSSOID": "SupervisorSSOID",
                "SupervisorName": "SupervisorName",
                "DWSStartDate": "DWSStartDate",
                "DWSMonday": "DWSMonday",
                "DWSTuesday": "DWSTuesday",
                "DWSWednesday": "DWSWednesday",
                "DWSThursday": "DWSThursday",
                "DWSFriday": "DWSFriday",
                "DWSSaturday": "DWSSaturday",
                "DWSSunday": "DWSSunday",
                "TerminationEffectiveDate": "TerminationEffectiveDate",
                "IndustryFocusGroup": "IndustryFocusGroup",
                "LegalEntity": "LegalEntity",
                "ContractID": "ContractID",
                "ContractType": "ContractType",
                "RadiationFlag": "RadiationFlag",
                "PositionCapacity": "PositionCapacity",
                "PreviousExperience": "PreviousExperience",
                "OvertimeEligibility": "OvertimeEligibility",
                "SuspendAssignmentCategory": "SuspendAssignmentCategory",
                "Payroll": "Payroll",
                "HealthcareProductLineEIT": "HealthcareProductLineEIT",
                "JobType": "JobType",
                "CareerBand": "CareerBand",
                "AdjustedServiceDate": "AdjustedServiceDate",
                "Work": "Work",
                "HRMSSOID": "HRMSSOID",
                "HRMName": "HRMName",
                "SpecialWorkSchedule": "SpecialWorkSchedule",
                "EducationLevel": "EducationLevel",
                "WorkLocation": "WorkLocation",
                "AssignmentEffectiveDate": "AssignmentEffectiveDate",
                "HireEffectiveDate": "HireEffectiveDate",
                "RevTermEffectiveDate": "RevTermEffectiveDate"
            }
        )

        get_all_user_custom_fields_7 = rail.RepliconServiceOperator(
            task_id='get_all_user_custom_fields_7',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        log_suspend_assignment_category_uri_8 = rail.PythonOperator(
            task_id='log_suspend_assignment_category_uri_8',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_user_custom_fields_7'), 'displayText', "Suspend Assignment Category", 'uri')
        )

        get_all_custom_field_drop_down_optionsfor_suspend_assignment_category_getvalues_9 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_optionsfor_suspend_assignment_category_getvalues_9',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_suspend_assignment_category_uri_8') }}"
            }
        )

        create_list_currentvaluesfrom_customfield_13 = rail.CreateCollectionOperator(
            task_id='create_list_currentvaluesfrom_customfield_13',
            source="{{ result('get_all_custom_field_drop_down_optionsfor_suspend_assignment_category_getvalues_9') | to_json }}",
            name="existingassignmentcategoryvalues",
        )

        query_list_newvaluestoadd_14 = rail.QueryCollectionOperator(
            task_id='query_list_newvaluestoadd_14',
            query="""SELECT DISTINCT inputdata.SuspendAssignmentCategory FROM  inputdata WHERE (LOWER( inputdata.SuspendAssignmentCategory) NOT IN (SELECT LOWER( existingassignmentcategoryvalues.displayText) FROM  existingassignmentcategoryvalues)) AND  NULLIF(inputdata.SuspendAssignmentCategory,'') IS NOT NULL""",
        )

        if_first_assignmentcategory_present_15 = rail.IfOperator(
            task_id='if_first_assignmentcategory_present_15',
            test='{{ result("query_list_newvaluestoadd_14", "length") > 0 }}',
            yes_task="put_drop_down_optionsfor_suspend_assignment_category_24",
            no_task="catch_25",
        )

        def get_payrate_list():
            payrate_option_list = []
            existing_payrate_list = rail.result(
                'get_all_custom_field_drop_down_optionsfor_suspend_assignment_category_getvalues_9')
            for existing_payrate in existing_payrate_list:
                payrate_option_list.append({
                    "target": {
                        "uri": existing_payrate['uri'],
                        "name": existing_payrate['displayText']
                    },
                    "name": existing_payrate['displayText'],
                    "isEnabled": existing_payrate['isEnabled']
                })
            new_payrate_list = get_data_from_document(
                rail.result('query_list_newvaluestoadd_14'))
            for new_payrate in new_payrate_list:
                payrate_option_list.append({
                    "target": {
                        "uri": null,
                        "name": null
                    },
                    "name": new_payrate['SuspendAssignmentCategory'],
                    "isEnabled": True
                })
            return payrate_option_list

        put_drop_down_optionsfor_suspend_assignment_category_24 = rail.RepliconServiceOperator(
            task_id='put_drop_down_optionsfor_suspend_assignment_category_24',
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('log_suspend_assignment_category_uri_8'),
                "customFieldDropDownOptionUris":  get_payrate_list()
            }
        )

        catch_25 = rail.EmptyOperator(
            task_id='catch_25',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_25
        can_run_batch_task >> rail.Label('No') >> create_list_5 >> get_all_user_custom_fields_7 >> \
            log_suspend_assignment_category_uri_8 >> get_all_custom_field_drop_down_optionsfor_suspend_assignment_category_getvalues_9 >> \
            create_list_currentvaluesfrom_customfield_13 >> query_list_newvaluestoadd_14 >> if_first_assignmentcategory_present_15
        if_first_assignmentcategory_present_15 >> rail.Label(
            'Yes') >> put_drop_down_optionsfor_suspend_assignment_category_24 >> catch_25
        if_first_assignmentcategory_present_15 >> rail.Label(
            'No') >> catch_25 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
