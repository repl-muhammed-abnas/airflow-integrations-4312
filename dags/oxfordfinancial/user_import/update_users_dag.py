from datetime import datetime, timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/user_import/config.py


# pylint: disable=too-many-statements
def create_updateuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_user_import_update_users_{config.instance}',
        description=f'Update Users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_enddate_startdate_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_enddate_startdate_present',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_enddate_startdate_present = rail.IfOperator(
            task_id='is_enddate_startdate_present',
            test="{{ dag_run.conf.End_Date | sn | is_truthy \
                and dag_run.conf.Start_Date | sn | is_truthy }}",
            yes_task="update_startdate_enddate",
            no_task="update_lastname"
        )

        def get_replicon_datetime_obj(date_str, fmt='%m/%d/%Y'):
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }
        update_startdate_enddate = rail.RepliconServiceOperator(
            task_id='update_startdate_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_replicon_datetime_obj(dag_run.conf['Start_Date']),
                    "endDate": get_replicon_datetime_obj(dag_run.conf['End_Date'])
                }
            }
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.Last_Name }}"
            }
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.First_Name }}"
            }
        )

        update_sf_id = rail.RepliconServiceOperator(
            task_id='update_sf_id',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.sfid_uri }}",
                "value": "{{ dag_run.conf.SF_18_Digit_ID }}"
            }
        )

        update_initials = rail.RepliconServiceOperator(
            task_id='update_initials',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.initials_uri }}",
                "value": "{{ dag_run.conf.Initials }}"
            }
        )

        update_title = rail.RepliconServiceOperator(
            task_id='update_title',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.title_uri }}",
                "value": "{{ dag_run.conf.Title }}"
            }
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        get_required_employeetype_user = rail.RepliconServiceOperator(
            task_id='get_required_employeetype_user',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['Employee_Type'], 'uri', '')
        )

        is_employeetype_to_update = rail.IfOperator(
            task_id='is_employeetype_to_update',
            test="{{ result('get_required_employeetype_user') | is_truthy }}",
            yes_task='update_employeetype_user',
            no_task='get_user_details'
        )

        update_employeetype_user = rail.RepliconServiceOperator(
            task_id='update_employeetype_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('get_required_employeetype_user') }}"
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        is_supervisor_not_same = rail.IfOperator(
            task_id='is_supervisor_not_same',
            test="{{ result('get_user_details') | attr_or_default('supervisor.displayText', '') != dag_run.conf.Supervisor }}",
            yes_task="get_supervisor_useruri",
            no_task="is_department_present"
        )

        def get_supervisor_uri(response, dag_run):
            filtered_supervisor = list(filter(lambda x: x['cells'][0]['textValue'] == dag_run.conf['Supervisor'],
                                              response['rows'])) if response['rows'] else []
            if filtered_supervisor:
                return rail.smartjoin_by_delim([x['cells'][0]['uri'] for x in filtered_supervisor], ' ') if response['rows'] else ''
            return ''
        get_supervisor_useruri = rail.RepliconServiceOperator(
            task_id='get_supervisor_useruri',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user-name"
                ]
            },
            data_handler=get_supervisor_uri
        )

        is_supervisor_present_in_replicon = rail.IfOperator(
            task_id='is_supervisor_present_in_replicon',
            test="{{ result('get_supervisor_useruri') | is_truthy }}",
            yes_task="update_supervisor_with_today",
            no_task="is_department_present"
        )

        def get_today_date():
            now = datetime.utcnow()
            return {
                'year': now.year,
                'month': now.month,
                'day': now.day
            }
        update_supervisor_with_today = rail.RepliconServiceOperator(
            task_id='update_supervisor_with_today',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('get_supervisor_useruri'),
                "dateRange": {
                    "startDate": get_today_date()
                }
            }
        )

        is_department_present = rail.IfOperator(
            task_id='is_department_present',
            test="{{ dag_run.conf.Department | sn | is_truthy }}",
            yes_task="process_department",
            no_task="write_update_user_log"
        )

        process_department = rail.EmptyOperator(
            task_id='process_department'
        )

        is_locationuri_present = rail.IfOperator(
            task_id='is_locationuri_present',
            test="{{ dag_run.conf.location_uri | is_truthy }}",
            yes_task="process_current_department",
            no_task="write_update_user_log"
        )

        process_current_department = rail.EmptyOperator(
            task_id='process_current_department'
        )

        is_current_department_present = rail.IfOperator(
            task_id='is_current_department_present',
            test="{{ dag_run.conf.current_department | is_truthy and \
                dag_run.conf.Department != dag_run.conf.current_department }}",
            yes_task="get_put_locationschedule_user",
            no_task="put_initial_location_schedule_user"
        )

        def get_locationschedule_request(response, dag_run):
            initial_location_list = []
            subsequent_list = []
            if response:
                for item in response:
                    if item.get('effectiveDate') and item['effectiveDate'].get('day'):
                        subsequent_list.append({
                            "location": {
                                "uri": item['location']['uri']
                            },
                            "effectiveDate": item['effectiveDate']
                        })
                    else:
                        initial_location_list.append({
                            "location": {
                                "uri": item['location']['uri']
                            }
                        })

            def get_tomorrow_date():
                tomorrow = datetime.utcnow() + timedelta(days=1)
                return {
                    'year': tomorrow.year,
                    'month': tomorrow.month,
                    'day': tomorrow.day
                }
            subsequent_list.append({
                "location": {
                    "uri": dag_run.conf['location_uri']
                },
                "effectiveDate": get_tomorrow_date()
            })
            return initial_location_list + subsequent_list
        get_put_locationschedule_user = rail.RepliconServiceOperator(
            task_id='get_put_locationschedule_user',
            endpoint="/services/LocationService1.svc/GetLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_locationschedule_request
        )

        put_location_schedule_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_put_locationschedule_user')
            }
        )

        put_initial_location_schedule_user = rail.RepliconServiceOperator(
            task_id='put_initial_location_schedule_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": '{{ dag_run.conf.location_uri }}'
                        }
                    }
                ]
            }
        )

        write_update_user_log = rail.WriteLogOperator(
            task_id='write_update_user_log',
            log="{{ dag_run.conf.log }}",
            message="Updated",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.Active_Directory_Login }}",
                "sf18digitid": "{{ dag_run.conf.SF_18_Digit_ID }}",
                "status": "Success",
                "reason": "Updated"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties={
                "loginname": "{{ dag_run.conf.Active_Directory_Login }}",
                "sf18digitid": "{{ dag_run.conf.SF_18_Digit_ID }}",
                "status": "Error",
                "reason": 'Update user - {{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> is_enddate_startdate_present
        is_enddate_startdate_present >> rail.Label(
            'Yes') >> update_startdate_enddate >> update_lastname
        is_enddate_startdate_present >> rail.Label(
            'No') >> update_lastname
        update_lastname >> update_firstname >> update_sf_id >> update_initials >> update_title >> update_email >> \
            get_required_employeetype_user >> is_employeetype_to_update
        is_employeetype_to_update >> rail.Label(
            'Yes') >> update_employeetype_user >> get_user_details
        is_employeetype_to_update >> rail.Label(
            'No') >> get_user_details
        get_user_details >> is_supervisor_not_same
        is_supervisor_not_same >> rail.Label(
            'Yes') >> get_supervisor_useruri >> is_supervisor_present_in_replicon
        is_supervisor_present_in_replicon >> rail.Label(
            'Yes') >> update_supervisor_with_today >> is_department_present
        is_supervisor_present_in_replicon >> rail.Label(
            'No') >> is_department_present
        is_supervisor_not_same >> rail.Label(
            'No') >> is_department_present
        is_department_present >> rail.Label(
            'Yes') >> process_department >> is_locationuri_present
        is_locationuri_present >> rail.Label(
            'Yes') >> process_current_department >> is_current_department_present
        is_current_department_present >> rail.Label(
            'Yes') >> get_put_locationschedule_user >> put_location_schedule_user >> write_update_user_log
        is_current_department_present >> rail.Label(
            'No') >> put_initial_location_schedule_user >> write_update_user_log
        is_locationuri_present >> rail.Label(
            'No') >> write_update_user_log
        is_department_present >> rail.Label(
            'No') >> write_update_user_log

        write_update_user_log >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_updateuser_dag)
