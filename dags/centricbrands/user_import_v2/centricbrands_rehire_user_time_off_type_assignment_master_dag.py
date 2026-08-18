
from datetime import timedelta, datetime
from centricbrands.user_import_v2.mappers.centric_brands_time_off_type_assignment_mapper import centric_brands_time_off_type_assignment
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'centricbrands_user_import_rehire_user_time_off_type_assignment_master_{config.instance}_v2',
        description=f'CentricBrands Rehire User - Time Off Type assignment master {config.instance}_v2',
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
            no_task='check_location_hk'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_location_hk',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        check_location_hk = rail.IfOperator(
            task_id='check_location_hk',
            test='''{{dag_run.conf.location | lower == 'hong kong'}}''',
            yes_task='search_matching_timeoff_hk',
            no_task='check_location_china'
        )

        search_matching_timeoff_hk = rail.PythonOperator(
            task_id='search_matching_timeoff_hk',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (dag_run.conf['location']).lower() and entry['hong_kong_levels'] == (
                dag_run.conf['hongkonglevels']).lower(), centric_brands_time_off_type_assignment))
        )

        check_location_china = rail.IfOperator(
            task_id='check_location_china',
            test='''{{dag_run.conf.location | lower == 'china'}}''',
            yes_task='search_matching_timeoff_china',
            no_task='search_timeofftype_for_user'
        )

        search_matching_timeoff_china = rail.PythonOperator(
            task_id='search_matching_timeoff_china',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (dag_run.conf['location']).lower() and entry['state'] == (
                dag_run.conf['stateprovince']).lower(), centric_brands_time_off_type_assignment))
        )

        search_timeofftype_for_user = rail.PythonOperator(
            task_id='search_timeofftype_for_user',
            python_callable=lambda dag_run: list(filter(lambda entry: entry['country'] == (dag_run.conf['location']).lower() and entry['state'] == (
                dag_run.conf['stateprovince']).lower() and entry['employeetype'] == (
                dag_run.conf['employeetype']).lower(), centric_brands_time_off_type_assignment))
        )

        required_mapper_record_with_timeoff_type = rail.PythonOperator(
            task_id='required_mapper_record_with_timeoff_type',
            python_callable=lambda: rail.result('search_matching_timeoff_hk') or rail.result(
                'search_matching_timeoff_china') or rail.result('search_timeofftype_for_user')
        )

        if_timeofftype_present = rail.IfOperator(
            task_id='if_timeofftype_present',
            test=lambda: bool(rail.result(
                'required_mapper_record_with_timeoff_type')),
            yes_task="get_integrationdate_object",
            no_task="catch_error",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year,
                'datestring': datestring
            }

        get_integrationdate_object = rail.PythonOperator(
            task_id='get_integrationdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['integrationdate'] if dag_run.conf['integrationdate'] else dag_run.conf['startdate'])
        )

        get_startdate_object = rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['startdate'])
        )

        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.uri }}"
            }
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_timeofftypes_to_assign = rail.PythonOperator(
            task_id='get_timeofftypes_to_assign',
            python_callable=lambda: ((rail.result('required_mapper_record_with_timeoff_type'))[
                                     0]['timeofftypes']).split('|')
        )

        def get_timeofftypes_with_uri():
            timeofftypes = rail.result('get_timeofftypes_to_assign')
            all_timeofftypes = rail.result('get_all_time_off_types')
            timeoffs = [{
                'name': timeoff,
                'uri': rail.find_first_by_attr_and_get_attr(all_timeofftypes, 'displayText', timeoff.strip(), 'uri', '')
            } for timeoff in timeofftypes]
            return {
                'uris': [timeoff['uri'] for timeoff in timeoffs],
                'timeoffs': timeoffs
            }

        get_timeofftypes_to_assign_with_uri = rail.PythonOperator(
            task_id='get_timeofftypes_to_assign_with_uri',
            python_callable=get_timeofftypes_with_uri
        )

        assign_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['uri'],
                "timeOffTypeUris": rail.result('get_timeofftypes_to_assign_with_uri')['uris']
            }
        )

        if_location_china_hongkong = rail.IfOperator(
            task_id='if_location_china_hongkong',
            test='''{{dag_run.conf.location | lower == 'china' or dag_run.conf.location | lower == 'hong kong'}}''',
            yes_task='catch_error',
            no_task='get_startingbalancesetto_uri'
        )

        get_startingbalancesetto_uri = rail.RepliconServiceOperator(
            task_id='get_startingbalancesetto_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_preventbalanceoverdraw_uri = rail.RepliconServiceOperator(
            task_id='get_preventbalanceoverdraw_uri',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        trigger_child_rehire_user_timeoffpolicy_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_rehire_user_timeoffpolicy_assignment',
            retries=0,
            items=lambda: rail.result('get_timeofftypes_to_assign_with_uri')[
                'timeoffs'],
            trigger_dag_id=f'centricbrands_user_import_rehire_user_time_off_policy_assignment_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "uri": "{{ dag_run.conf.uri }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "email": "{{ dag_run.conf.email }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "authenticationtype": "{{ dag_run.conf.authenticationtype }}",
                "departmentname": "{{ dag_run.conf.departmentname }}",
                "licenseseats": "{{ dag_run.conf.licenseseats }}",
                "isloginenabled": "{{ dag_run.conf.isloginenabled }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "integrationdate": "{{ dag_run.conf.integrationdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "userpermission": "{{ dag_run.conf.userpermission }}",
                "supervisorpermission": "{{ dag_run.conf.supervisorpermission }}",
                "teammanager": "{{ dag_run.conf.teammanager }}",
                "payrollmanager": "{{ dag_run.conf.payrollmanager }}",
                "administratorpermission": "{{ dag_run.conf.administratorpermission }}",
                "location": "{{ dag_run.conf.location }}",
                "locationeffectivedate": "{{ dag_run.conf.locationeffectivedate }}",
                "team": "{{ dag_run.conf.team }}",
                "teameffectivedate": "{{ dag_run.conf.teameffectivedate }}",
                "stateprovince": "{{ dag_run.conf.stateprovince }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "supervisorstartdate": "{{ dag_run.conf.supervisorstartdate }}",
                "timeofftemplate": "{{ dag_run.conf.timeofftemplate }}",
                "timeoffapprovalpath": "{{ dag_run.conf.timeoffapprovalpath }}",
                "holidaycalendar": "{{ dag_run.conf.holidaycalendar }}",
                "schedule": "{{ dag_run.conf.schedule }}",
                "scheduleeffectivedate": "{{ dag_run.conf.scheduleeffectivedate }}",
                "timeofftypename": "{{ item.name }}",
                "timeoffuri": "{{ item.uri }}",
                "startingbalancesettouri": "{{ result('get_startingbalancesetto_uri') }}",
                "preventbalanceoverdrawuri": "{{ result('get_preventbalanceoverdraw_uri') }}"
            }
        )

        wait_for_child_rehire_user_timeoffpolicy_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_rehire_user_timeoffpolicy_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_rehire_user_timeoffpolicy_assignment") }}'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> check_location_hk
        check_location_hk >> rail.Label(
            'Yes') >> search_matching_timeoff_hk >> required_mapper_record_with_timeoff_type
        check_location_hk >> rail.Label('No') >> check_location_china
        check_location_china >> rail.Label(
            'Yes') >> search_matching_timeoff_china >> required_mapper_record_with_timeoff_type
        check_location_china >> rail.Label(
            'No') >> search_timeofftype_for_user >> required_mapper_record_with_timeoff_type
        required_mapper_record_with_timeoff_type >> if_timeofftype_present
        if_timeofftype_present >> rail.Label(
            'Yes') >> get_integrationdate_object >> get_startdate_object >> get_user_time_off_type_policy_summary
        get_user_time_off_type_policy_summary >> get_all_time_off_types >> get_timeofftypes_to_assign >> get_timeofftypes_to_assign_with_uri
        get_timeofftypes_to_assign_with_uri >> assign_timeofftypes >> if_location_china_hongkong
        if_location_china_hongkong >> rail.Label('Yes') >> catch_error
        if_location_china_hongkong >> rail.Label(
            'No') >> get_startingbalancesetto_uri
        get_startingbalancesetto_uri >> get_preventbalanceoverdraw_uri
        get_preventbalanceoverdraw_uri >> trigger_child_rehire_user_timeoffpolicy_assignment >> wait_for_child_rehire_user_timeoffpolicy_assignment
        wait_for_child_rehire_user_timeoffpolicy_assignment >> catch_error
        if_timeofftype_present >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
