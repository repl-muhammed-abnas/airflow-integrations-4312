
from datetime import timedelta
import itertools
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_czech_process_each_user_dag_{config.instance}',
        description=f'GE Czech Process Each User {config.instance}',
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
            no_task='if_column_blank_skip_processing_23'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_column_blank_skip_processing_23',
            end_task='ey_user_import_logs_add_entry_36',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_column_blank_skip_processing_23 = rail.IfOperator(
            task_id='if_column_blank_skip_processing_23',
            test='''{{ dag_run.conf.LegalEntity | is_falsy  or dag_run.conf.OHRID | is_falsy }}''',
            yes_task="ey_user_import_logs_add_entry_24",
            no_task="if_column_19_present_process_records_25",
        )

        def get_validation_message(item):
            validations = []
            if not item['OHRID']:
                validations.append("HR ID Not present in feed file")
            if not item['PositionCapacity']:
                validations.append("Legal Entity Not present in feed file")
            return rail.smartjoin_by_delim(validations, ';')

        ey_user_import_logs_add_entry_24 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_add_entry_24',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "jobid": get_dagrun_ecid(dag_run),
                "action": "Validation",
                "status": "Skipped",
                "OHRID": "",
                "details": get_validation_message(dag_run.conf)
            }
        )

        if_column_19_present_process_records_25 = rail.IfOperator(
            task_id='if_column_19_present_process_records_25',
            test='''{{ dag_run.conf.LegalEntity | is_truthy  and dag_run.conf.OHRID | is_truthy }}''',
            yes_task="search_users_27",
            no_task="ey_user_import_logs_add_entry_36",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_27 = rail.RepliconServicePageOperator(
            task_id='search_users_27',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['OHRID'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['OHRID'])
        )

        if_log_useruriiftheprofileexists_31_present_update_user_32 = rail.IfOperator(
            task_id='if_log_useruriiftheprofileexists_31_present_update_user_32',
            test='''{{ result('search_users_27') | is_truthy and result('search_users_27').useruri | is_truthy }}''',
            yes_task="trigger_dag_run_live_ge_czech_user_updateasync_33",
            no_task="trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35",
        )

        trigger_dag_run_live_ge_czech_user_updateasync_33 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_ge_czech_user_updateasync_33',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_czech_user_update_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "FirstName": dag_run.conf['EmployeeFirstName'],
                "LastName": dag_run.conf['EmployeeLastName'],
                "Email": dag_run.conf['EmployeeEmailAddress'],
                "JobPositionTitle": dag_run.conf['Job_PositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategoryEmployeeType": null,
                "DWSMonday": dag_run.conf['DWSMonday'],
                "DWSTuesday": dag_run.conf['DWSTuesday'],
                "DWSWednesday": dag_run.conf['DWSWednesday'],
                "DWSThursday": dag_run.conf['DWSThursday'],
                "DWSFriday": dag_run.conf['DWSFriday'],
                "DWSSaturday": dag_run.conf['DWSSaturday'],
                "DWSSunday": dag_run.conf['DWSSunday'],
                "ScheduleStartDate": dag_run.conf['DWSStartDate'],
                "ScheduleEndDate": null,
                "UserURI": rail.result('search_users_27')['useruri'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "reverseterminationeffectivedate": dag_run.conf['RevTermEffectiveDate'],
                "terminationeffectivedate": dag_run.conf['TerminationEffectiveDate'],
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "SuspendAssignmentCategory": dag_run.conf['SuspendAssignmentCategory'],
                "Assignmenteffectivedate": dag_run.conf['Assignmenteffectivedate'],
                "legalentityhiredate": dag_run.conf['LegalEntityHireDate'],
                "Hireeffectivedate": dag_run.conf['Hireeffectivedate'],
                "employeegender": null,
                "Maritalstatus": null,
                "legacypayrollid": dag_run.conf['LegacyPayrollID'],
                "industryfocusgroup": dag_run.conf['IndustryFocusGroup'],
                "contractid": dag_run.conf['ContractID'],
                "radiationflag": dag_run.conf['RadiationFlag'],
                "positioncapacity": dag_run.conf['PositionCapacity'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_live_ge_czech_user_updateasync_33 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_ge_czech_user_updateasync_33',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_ge_czech_user_updateasync_33") }}'
        )

        trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_czech_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['EmployeeFirstName'],
                "EmployeeLastName": dag_run.conf['EmployeeLastName'],
                "EmployeeEmailAddress": dag_run.conf['EmployeeEmailAddress'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['LegalEntityHireDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "Employeegender": null,
                "MaritalStatus": null,
                "JobPositionTitle": dag_run.conf['Job_PositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "Assignmentcategory": null,
                "DWSStartDate": dag_run.conf['DWSStartDate'],
                "DWSEndDate": null,
                "DWSMonday": dag_run.conf['DWSMonday'],
                "DWSTuesday": dag_run.conf['DWSTuesday'],
                "DWSWednesday": dag_run.conf['DWSWednesday'],
                "DWSThursday": dag_run.conf['DWSThursday'],
                "DWSFriday": dag_run.conf['DWSFriday'],
                "DWSSaturday": dag_run.conf['DWSSaturday'],
                "DWSSunday": dag_run.conf['DWSSunday'],
                "TerminationEffectiveDate": dag_run.conf['TerminationEffectiveDate'],
                "IndustryFocusGroup": dag_run.conf['IndustryFocusGroup'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "ContractID": dag_run.conf['ContractID'],
                "RadiationFlag": dag_run.conf['RadiationFlag'],
                "PositionCapacity": dag_run.conf['PositionCapacity'],
                "Educationperiodsstartdate": null,
                "Educationperiodsenddate": null,
                "Previousemploymentsperiodsstartdate": null,
                "Previousemploymentsperiodsenddate": null,
                "Departmentalstom": null,
                "Salarybasis": null,
                "Overtimeeligibility": null,
                "SuspendAssignmentCategory": dag_run.conf['SuspendAssignmentCategory'],
                "Dateofbirth": null,
                "Payroll": dag_run.conf['Payroll'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "JobType": dag_run.conf['JobType'],
                "CareerBand": dag_run.conf['CareerBand'],
                "AdjustedServiceDate": dag_run.conf['AdjustedServiceDate'],
                "Work": dag_run.conf['Work'],
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "SpecialWorkSchedule": dag_run.conf['SpecialWorkSchedule'],
                "EducationLevel": dag_run.conf['EducationLevel'],
                "Worktimesystem": null,
                "Subbiz": null,
                "Contractattributeannualvacationeligibility": null,
                "Locationname": null,
                "Assignmenteffectivedate": dag_run.conf['Assignmenteffectivedate'],
                "Hireeffectivedate": dag_run.conf['Hireeffectivedate'],
                "revtermeffectivedate": dag_run.conf['RevTermEffectiveDate'],
                "type": "Add",
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35") }}'
        )

        process_users = rail.EmptyOperator(
            task_id="process_users"
        )

        ey_user_import_logs_add_entry_36 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_add_entry_36',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "Add/Update",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> ey_user_import_logs_add_entry_36
        can_run_batch_task >> rail.Label('No') >> \
            if_column_blank_skip_processing_23
        if_column_blank_skip_processing_23 >> rail.Label(
            'No') >> if_column_19_present_process_records_25
        if_column_19_present_process_records_25 >> rail.Label(
            'No') >> ey_user_import_logs_add_entry_36
        if_column_19_present_process_records_25 >> rail.Label(
            'Yes') >> search_users_27 >> if_log_useruriiftheprofileexists_31_present_update_user_32
        if_log_useruriiftheprofileexists_31_present_update_user_32 >> rail.Label('No') >> \
            trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35 >> wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_35 >> process_users
        if_log_useruriiftheprofileexists_31_present_update_user_32 >> rail.Label('Yes') >> \
            trigger_dag_run_live_ge_czech_user_updateasync_33 >> \
            wait_for_completion_trigger_dag_run_live_ge_czech_user_updateasync_33 >> \
            process_users >> ey_user_import_logs_add_entry_36
        if_column_blank_skip_processing_23 >> rail.Label('Yes') >> ey_user_import_logs_add_entry_24 >> \
            ey_user_import_logs_add_entry_36 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
