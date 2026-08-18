from datetime import timedelta
from airflow.models import Variable
import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_update_disableuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_update_user_disabledstatus_{config.instance}',
        description=f'Child_Disable_User in Excluded job codes_CR.14.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_updateuser_disabledstatus'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_updateuser_disabledstatus',
            end_task='catch_and_log_errors',
        )

        process_updateuser_disabledstatus = rail.EmptyOperator(
            task_id='process_updateuser_disabledstatus'
        )

        generate_userreport = rail.RepliconServiceOperator(
            task_id='generate_userreport',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data=lambda dag_run: {
                'reportUri': config.user_report_uri,
                'filterValues': [
                    {
                        'reportFilterUri': config.user_report_filter_uri,
                        'value': dag_run.conf['useruri'].split(':')[-1]
                    }
                ],
                'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
            }
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('generate_userreport').payload }}",
            headers=['User First Name', 'User Last Name', 'User Email', 'User Status', 'User Start Date',
                     'User End Date', 'User Supervisor Name (Current)', 'User Department Name', 'Employee ID',
                     'Login Name', 'Employee Type', 'Punch Entry Policy Name', 'Service Date', 'Student Worker',
                     'Job Code', 'Job_Title', 'Paygroup (Current)', 'Division', 'Salary/Hourly', 'Regular/Temp',
                     'Full/Part Time', 'Active/Leave Status', 'Home State', 'FLSA Status', 'File Number', 'Rehire Date',
                     'Colleague D Number', 'CoCode', 'Holiday Calendar', 'Time Zone', 'Authentication Type',
                     'Timesheet Approval Path', 'Time Off Approval Path', 'Timesheet Period Type', 'Timesheet Template',
                     'Time Off Template', 'Schedule Name (Current)', 'Batch ID', 'Work Week', 'supervisor uri', 'Pay Rule Name',
                     'Standard Hours', 'Department Number', 'Work Location']
        )

        parse_csv_user_data = rail.PythonOperator(
            task_id='parse_csv_user_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv'))[0]
        )

        is_repliconuserstatus_not_disabled = rail.IfOperator(
            task_id='is_repliconuserstatus_not_disabled',
            test="{{ result('parse_csv_user_data')['User Status'] != 'Disabled' }}",
            yes_task="disable_login",
            no_task="is_repliconuserstatus_disabled",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        remove_templates = rail.RepliconServiceOperator(
            task_id='remove_templates',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUris": []
            }
        )

        trigger_disable_user_time_off_cr_14 = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user_time_off_cr_14',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_disable_user_timeoff_crv14.0_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "jobtitle": "{{ dag_run.conf.jobtitle }}",
                "managerindicator": "{{ dag_run.conf.managerindicator }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "division": "{{ dag_run.conf.division }}",
                "salaryhourly": "{{ dag_run.conf.salaryhourly }}",
                "regulartemp": "{{ dag_run.conf.regulartemp }}",
                "fullparttime": "{{ dag_run.conf.fullparttime }}",
                "activeleavestatus": "{{ dag_run.conf.activeleavestatus }}",
                "supervisor": "{{ dag_run.conf.supervisor }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "homestate": "{{ dag_run.conf.homestate }}",
                "standardhours": "{{ dag_run.conf.standardhours }}",
                "flsastatus": "{{ dag_run.conf.flsastatus }}",
                "filenumber": "{{ dag_run.conf.filenumber }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "colleaguednumber": "{{ dag_run.conf.colleaguednumber }}",
                "newmapperlookup": "",
                "worklocation": "{{ dag_run.conf.worklocation }}",
                "userstatus": "",
                "useruri": "{{ dag_run.conf.useruri }}",
                "rooturl": "{{ dag_run.conf.rooturl }}"
            }
        )

        remove_timeoff_types = rail.RepliconServiceOperator(
            task_id='remove_timeoff_types',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUris": []
            }
        )

        is_jobcode_present = rail.IfOperator(
            task_id='is_jobcode_present',
            test='{{ dag_run.conf.jobcode | is_truthy }}',
            yes_task="get_user_customfieldgroupuri",
            no_task="write_disableuser_excludedjobcode_log",
        )

        get_user_customfieldgroupuri = rail.RepliconServiceOperator(
            task_id='get_user_customfieldgroupuri',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "{{ result('get_user_customfieldgroupuri').uri }}"
            },
            data_handler=lambda response: {
                'job_code': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Code', 'uri', '')
            }
        )

        get_jobcode_dropdown = rail.RepliconServiceOperator(
            task_id='get_jobcode_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'jobcode'], 'uri', '')
        )

        is_jobcode_dropdown_present = rail.IfOperator(
            task_id='is_jobcode_dropdown_present',
            test="{{ result('get_jobcode_dropdown') | is_truthy }}",
            yes_task="update_jobcode_udf",
            no_task="write_disableuser_excludedjobcode_log",
        )

        update_jobcode_udf = rail.RepliconServiceOperator(
            task_id='update_jobcode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').job_code }}",
                "customFieldDropDownOptionUri": "{{ result('get_jobcode_dropdown') }}"
            }
        )

        write_disableuser_excludedjobcode_log = rail.WriteLogOperator(
            task_id='write_disableuser_excludedjobcode_log',
            log='{{ dag_run.conf.log }}',
            message="Disabled - User in Excluded Job code list",
            severity="Disabled - User in Excluded Job code list",
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': "Disabled - User in Excluded Job code list",
                "failure_reason": ""
            }
        )

        is_repliconuserstatus_disabled = rail.IfOperator(
            task_id='is_repliconuserstatus_disabled',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Disabled' }}",
            yes_task="write_disableuser_excludedjobcode_log2",
            no_task="catch_and_log_errors",
        )

        write_disableuser_excludedjobcode_log2 = rail.WriteLogOperator(
            task_id='write_disableuser_excludedjobcode_log2',
            log='{{ dag_run.conf.log }}',
            message="User already Disabled - User in Excluded Job code list",
            severity="User already Disabled - User in Excluded Job code list",
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': "User already Disabled - User in Excluded Job code list",
                "failure_reason": ""
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            severity='Error',
            message="{{ get_error_message() }}",
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Error',
                # pylint: disable=line-too-long
                'failure_reason': "User \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" not Disabled: {{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> process_updateuser_disabledstatus

        process_updateuser_disabledstatus >> generate_userreport >> parse_csv >> parse_csv_user_data >> \
            is_repliconuserstatus_not_disabled

        is_repliconuserstatus_not_disabled >> rail.Label(
            'Yes') >> disable_login >> remove_templates >> trigger_disable_user_time_off_cr_14 >> \
            remove_timeoff_types >> is_jobcode_present
        is_jobcode_present >> rail.Label(
            'Yes') >> get_user_customfieldgroupuri >> get_required_user_customfields >> \
            get_jobcode_dropdown >> is_jobcode_dropdown_present
        is_jobcode_dropdown_present >> rail.Label(
            'Yes') >> update_jobcode_udf >> write_disableuser_excludedjobcode_log
        is_jobcode_dropdown_present >> rail.Label(
            'No') >> write_disableuser_excludedjobcode_log
        is_jobcode_present >> rail.Label(
            'No') >> write_disableuser_excludedjobcode_log
        write_disableuser_excludedjobcode_log >> is_repliconuserstatus_disabled
        is_repliconuserstatus_not_disabled >> rail.Label(
            'No') >> is_repliconuserstatus_disabled
        is_repliconuserstatus_disabled >> rail.Label(
            'Yes') >> write_disableuser_excludedjobcode_log2 >> catch_and_log_errors
        is_repliconuserstatus_disabled >> rail.Label(
            'No') >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_update_disableuser_child_dag)
