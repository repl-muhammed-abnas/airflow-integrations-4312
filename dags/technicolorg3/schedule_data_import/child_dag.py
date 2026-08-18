from datetime import datetime, timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_schedule_data_import_child_{config.instance}',
        description=f'Technicolor CETA Schedule Process  data - Child - V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        has_invalid_fd_status = rail.IfOperator(
            task_id='has_invalid_fd_status',
            test="{{ dag_run.conf.fd_status | lower != 'confirmed' and dag_run.conf.fd_status | lower != 'pencil' and dag_run.conf.fd_status | lower != 'cancelled' }}",
            yes_task="finish",
            no_task="validate_record"
        )

        def do_validate_record():
            conf = rail.get_current_context()['dag_run'].conf
            logs = []
            if conf.get('title', '').lower() != 'absence':
                if not conf['title']:
                    logs.append('Title value is blank')
                if not conf['mill_mpc']:
                    logs.append('MILL/MPC value is blank')
                if not conf['projectnumber']:
                    logs.append('Project Number value is blank')
                if not conf['role']:
                    logs.append('Role value is blank')
                if not conf['service']:
                    logs.append('Service value is blank')
                if not conf['description']:
                    logs.append('Description value is blank')
                if not conf['starttime']:
                    logs.append('Start Time value is blank')
                if not conf['endtime']:
                    logs.append('End Time value is blank')
                if not conf['resourcename']:
                    logs.append('Resource Name (Gloabl ID) value is blank')
                if not conf['resourcescheduleserviceID']:
                    logs.append('Resourcescheduler service ID value is blank')
                if not conf['fd_status']:
                    logs.append('Resource Name (Gloabl ID) value is blank')

            return ','.join(logs)

        validate_record = rail.PythonOperator(
            task_id='validate_record',
            python_callable=do_validate_record
        )

        has_invalid_record = rail.IfOperator(
            task_id='has_invalid_record',
            test="{{ result('validate_record') | is_truthy }}",
            yes_task="add_invalid_record_log_entry",
            no_task="has_resourcename",
        )

        add_invalid_record_log_entry = rail.WriteLogOperator(
            task_id='add_invalid_record_log_entry',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{  dag_run_ecid() }} has not been completed due to following missing value(s) - {{ result('validate_record') }}",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{  dag_run_ecid() }} has not been completed due to following missing value(s) - {{ result('validate_record') }}",
                'status': 'Exception',
            }
        )

        has_resourcename = rail.IfOperator(
            task_id='has_resourcename',
            test="{{ dag_run.conf.resourcename | is_truthy }}",
            yes_task="search_users",
            no_task="get_timeentry_oef",
        )

        def map_list_data(resp):
            data = resp.json()['d']['rows']
            return next(map(lambda item: {
                'uri': item['cells'][5]['uri'],
                'loginname': item['cells'][4]['textValue'],
                'startdate': item['cells'][2].get('dateValue'),
                'enddate': item['cells'][3].get('dateValue'),

            },
                filter(lambda item:
                       item['cells'][0]['textValue'] ==
                       str(rail.get_current_context(
                       )['dag_run'].conf['resourcename']) and
                       item['cells'][1]['boolValue'],
                       data)), None)

        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:start-date",
                    "urn:replicon:user-list-column:end-date",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": "{{ dag_run.conf.resourcename }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=map_list_data
        )

        has_invalid_user = rail.IfOperator(
            task_id='has_invalid_user',
            test="{{ result('search_users') | is_falsy }}",
            yes_task="add_invalid_user_log",
            no_task="has_invalid_daterange",
        )

        add_invalid_user_log = rail.WriteLogOperator(
            task_id='add_invalid_user_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the user with login name(Global ID) - {{dag_run.conf.resourcename}} is not exist/disabled in Replicon.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the user with login name(Global ID) - {{dag_run.conf.resourcename}} is doesn't exist or disabled in Replicon.",
                'status': 'Exception',
            }
        )

        def parse_date(str_datetime):
            return datetime.strptime(str_datetime.split('T')[0], "%Y-%m-%d")

        has_invalid_daterange = rail.IfOperator(
            task_id='has_invalid_daterange',
            test=lambda: (
                rail.result('search_users')['startdate'] and
                datetime(**rail.result('search_users')['startdate']) >
                parse_date(rail.get_current_context()[
                           'dag_run'].conf['starttime'])
            ) or
            (
                rail.result('search_users')['enddate'] and
                datetime(**rail.result('search_users')['enddate']) <
                parse_date(rail.get_current_context()[
                           'dag_run'].conf['endtime'])
            ),
            yes_task="add_invalid_daterange_log",
            no_task="get_timeentry_oef",
        )

        add_invalid_daterange_log = rail.WriteLogOperator(
            task_id='add_invalid_daterange_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the entry date is outside of the user's start & end date ranges.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the entry date is outside of the user's start & end date ranges.",
                'status': 'Exception',
            }
        )

        get_timeentry_oef = rail.RepliconServiceOperator(
            task_id="get_timeentry_oef",
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"}
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": str(dag_run.conf['projectnumber']) if dag_run.conf['projectnumber'] == 'absence' else str(int(dag_run.conf['projectnumber'])),
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        def get_replicon_date(date):
            if not date:
                return null
            return {
                'day': date.day,
                'month': date.month,
                'year': date.year,
            }

        get_user_group = rail.RepliconServiceOperator(
            task_id='get_user_group',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda: {
                "userUri": rail.result('search_users')['uri'],
                "dateRange": {
                    "startDate": get_replicon_date(parse_date(rail.get_current_context()['dag_run'].conf['starttime'])),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_timesheet = rail.RepliconServiceOperator(
            task_id="get_timesheet",
            endpoint='/services/TimesheetService1.svc/GettimesheetdetailsForDate',
            data=lambda: {
                "userUri": rail.result('search_users')['uri'],
                "date": get_replicon_date(parse_date(rail.get_current_context()['dag_run'].conf['starttime'])),
                "timesheetGetOptionUri": null
            }
        )

        has_no_timesheet = rail.IfOperator(
            task_id='has_no_timesheet',
            test="{{ result('get_timesheet') | is_falsy }}",
            yes_task="add_no_timesheet_log",
            no_task="get_timesheet_details",
        )

        add_no_timesheet_log = rail.WriteLogOperator(
            task_id='add_no_timesheet_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet for the received start date doesn't exist in Replicon.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet for the received start date doesn't exist in Replicon.",
                'status': 'Exception',
            }
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id="get_timesheet_details",
            endpoint='/services/TimesheetService1.svc/gettimesheetdetails',
            data=lambda: {
                "timesheetUri": rail.result('get_timesheet')['timesheet']['uri'],
            }
        )

        get_timeentry_revision_columns = rail.RepliconServiceOperator(
            task_id="get_timeentry_revision_columns",
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllColumns',
        )

        get_timeentry_revision_filters = rail.RepliconServiceOperator(
            task_id="get_timeentry_revision_filters",
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllFilterDefinitions',
        )

        has_no_usergroup = rail.IfOperator(
            task_id='has_no_usergroup',
            test="{{ result('get_user_group').employeeTypes | length == 0 or result('get_user_group').employeeTypes[0].employeeType.employeeType.displayText | is_falsy  or result('get_user_group').employeeTypes[0].employeeType.employeeType.displayText != 'Creative' }}",
            yes_task="add_no_usergroup_log",
            no_task="search_timeentry",
        )

        add_no_usergroup_log = rail.WriteLogOperator(
            task_id='add_no_usergroup_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the user's employee type is blank or is Non Creative in Replicon.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the user's employee type is blank or is Non Creative in Replicon.",
                'status': 'Exception',
            }
        )

        def map_duration(duration):
            if not duration:
                return null
            return round(duration['hours'] + (duration['minutes'] / 60) + (duration['seconds'] / 3600), 2)

        search_timeentry = rail.RepliconServiceOperator(
            task_id="search_timeentry",
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetData',
            data=lambda: {
                "columnUris": [
                    "urn:replicon:time-entry-revision-group-list-column:time-entry-revision-group",
                    "urn:replicon:time-entry-revision-group-list-column:entry-date",
                    "urn:replicon:time-entry-revision-group-list-column:hours",
                    "urn:replicon:time-entry-revision-group-list-column:project",
                    "urn:replicon:time-entry-revision-group-list-column:task",
                    "urn:replicon:time-entry-revision-group-list-column:comments",
                    "urn:replicon:time-entry-revision-group-list-column:approval-status",
                ]
                +
                list(map(lambda x: x['uri'], filter(lambda x: x['displayText'] == 'RSSID', rail.result(
                    'get_timeentry_revision_columns')[0]['columns']))),
                "page": "1",
                "pagesize": "100",
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": list(map(lambda x: x['uri'], filter(lambda x: x['name'] == 'RSSID', rail.result('get_timeentry_revision_filters'))))[0]
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "text":  int(rail.get_current_context()['dag_run'].conf['resourcescheduleserviceID'])
                        }
                    }
                }
            },
            data_handler=lambda data: list(map(lambda item: {
                "timeentryrevisiongroup": item['cells'][0]['uri'],
                "entrydate": item['cells'][1]['textValue'],
                "hours": item['cells'][2]['calendarDayDurationValue'],
                "projectname": item['cells'][3]['textValue'],
                "projecturi": item['cells'][3]['uri'],
                "taskname": item['cells'][4]['textValue'],
                "taskuri": item['cells'][4]['uri'],
                "timeentryid": item['cells'][7].get('textValue'),
                "comments": item['cells'][5].get('textValue'),
                "duration": map_duration(item['cells'][2]['calendarDayDurationValue']),
                "approvalstatus": item['cells'][6]['textValue']
            }, data['rows']))
        )

        has_cancelled_fd_status = rail.IfOperator(
            task_id='has_cancelled_fd_status',
            test="{{ dag_run.conf.fd_status | lower == 'cancelled' }}",
            yes_task="has_resource_scheduleserviceID",
            no_task="has_no_timesheet_uri",
        )

        has_resource_scheduleserviceID = rail.IfOperator(
            task_id='has_resource_scheduleserviceID',
            test="{{ result('search_timeentry') | find_first_by_attr_and_get_attr('timeentryid',dag_run.conf.resourcescheduleserviceID | int | string) | is_truthy }}",
            yes_task="has_no_open_timesheet",
            no_task="add_noentry_log",
        )

        has_no_open_timesheet = rail.IfOperator(
            task_id='has_no_open_timesheet',
            test=lambda: rail.result('get_timesheet_details')[
                'statusUri'].split(":")[-1] != "open",
            yes_task="add_invalid_timesheet_status",
            no_task="delete_timentry_revision",
        )

        add_invalid_timesheet_status = rail.WriteLogOperator(
            task_id='add_invalid_timesheet_status',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet is not in Not Submitted status.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet is not in Not Submitted status.",
                'status': 'Exception',
            }
        )

        delete_timentry_revision = rail.RepliconServiceOperator(
            task_id='delete_timentry_revision',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup",
            data={
                "timeEntryRevisionGroupUri": "{{ result('search_timeentry') | find_first_by_attr_and_get_attr('timeentryid',dag_run.conf.resourcescheduleserviceID | int |string,'timeentryrevisiongroup') }}"
            }
        )

        add_delete_log = rail.WriteLogOperator(
            task_id='add_delete_log',
            log="{{ result('create_log') }}",
            message="Required time entry deleted in Replicon",
            severity='Success',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "Required time entry deleted in Replicon",
                'status': 'Success',
            }
        )

        add_noentry_log = rail.WriteLogOperator(
            task_id='add_noentry_log',
            log="{{ result('create_log') }}",
            message="No entry found in mapper for the required Resourcescheduleservice ID",
            severity='Skipped',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "No entry found in mapper for the required Resourcescheduleservice ID",
                'status': 'Skipped',
            }
        )

        has_no_timesheet_uri = rail.IfOperator(
            task_id='has_no_timesheet_uri',
            test="{{ result('get_timesheet').timesheet.uri | is_falsy }}",
            yes_task="add_no_timesheeturi_log",
            no_task="has_invalid_timesheetstatus",
        )

        add_no_timesheeturi_log = rail.WriteLogOperator(
            task_id='add_no_timesheeturi_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet for the received start date doesn't exist in Replicon.",
            severity='Skipped',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet for the received start date doesn't exist in Replicon.",
                'status': 'Skipped',
            }
        )

        has_invalid_timesheetstatus = rail.IfOperator(
            task_id='has_invalid_timesheetstatus',
            test=lambda: rail.result('get_timesheet_details')[
                'statusUri'].split(":")[-1] != "open",
            yes_task="add_invalid_timesheet_status_log",
            no_task="has_projectnotpresent",
        )

        add_invalid_timesheet_status_log = rail.WriteLogOperator(
            task_id='add_invalid_timesheet_status_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet is not in Not Submitted status.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the timesheet is not in Not Submitted status.",
                'status': 'Exception',
            }
        )

        has_projectnotpresent = rail.IfOperator(
            task_id='has_projectnotpresent',
            test="{{ result('get_project_details')[0].projectDetails | is_falsy }}",
            yes_task="add_projectnotfound_log",
            no_task="has_uri_projecturiispresent",
        )

        add_projectnotfound_log = rail.WriteLogOperator(
            task_id='add_projectnotfound_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the project - {{dag_run.conf.title}} and number {{dag_run.conf.projectnumber}} doesn't exist in Replicon.",
            severity='Exception',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed since the project - {{dag_run.conf.title}} and number {{dag_run.conf.projectnumber}} doesn't exist in Replicon.",
                'status': 'Exception',
            }
        )

        has_uri_projecturiispresent = rail.IfOperator(
            task_id='has_uri_projecturiispresent',
            test="{{ result('get_project_details')[0].projectDetails | is_truthy }}",
            yes_task="has_no_absense",
            no_task="add_projectnotfound_log",
        )

        has_no_absense = rail.IfOperator(
            task_id='has_no_absense',
            test="{{ dag_run.conf.title | lower != 'absence' }}",
            yes_task="get_all_project_tasks",
            no_task="get_all_project_task3",
        )

        def map_task(data):
            tasks = []

            def add_child_taks(child_tasks):
                for child_task in child_tasks:
                    tasks.append(child_task['task'])
                    if child_task['childTasks']:
                        add_child_taks(child_task['childTasks'])

            for item in data:
                tasks.append(item['task'])
                add_child_taks(item['childTasks'])

            return {
                "tasklevel1_name": " ".join(list(map(lambda x: x['name'], filter(lambda x: x['name'] == rail.get_current_context()['dag_run'].conf['role'], tasks)))),
                "tasklevel1_uri": " ".join(list(map(lambda x: x['uri'], filter(lambda x: x['name'] == rail.get_current_context()['dag_run'].conf['role'], tasks)))),
                "tasklevel2_name": " ".join(list(map(lambda x: x['name'], filter(lambda x: x['displayText'] == rail.get_current_context()['dag_run'].conf['role'] + " / " + rail.get_current_context()['dag_run'].conf['service'], tasks)))),
                "tasklevel2_uri":  " ".join(list(map(lambda x: x['uri'], filter(lambda x: x['displayText'] == rail.get_current_context()['dag_run'].conf['role'] + " / " + rail.get_current_context()['dag_run'].conf['service'], tasks)))),
                "tasklevel3_name":  " ".join(list(map(lambda x: x['name'], filter(lambda x: x['displayText'] == rail.get_current_context()['dag_run'].conf['role'] + " / " + rail.get_current_context()['dag_run'].conf['service'] + " / " + rail.get_current_context()['dag_run'].conf['description'], tasks)))),
                "tasklevel3_uri": " ".join(list(map(lambda x: x['uri'], filter(lambda x: x['displayText'] == rail.get_current_context()['dag_run'].conf['role'] + " / " + rail.get_current_context()['dag_run'].conf['service'] + " / " + rail.get_current_context()['dag_run'].conf['description'], tasks)))),
            }

        get_all_project_tasks = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                "parentUri": "{{ result('get_project_details')[0].projectDetails.uri }}"
            },
            data_handler=map_task
        )

        has_no_tasklevel1_name = rail.IfOperator(
            task_id='has_no_tasklevel1_name',
            test="{{ result('get_all_project_tasks').tasklevel1_name | is_falsy }}",
            yes_task="put_new_task_level1",
            no_task="has_not_created_tasklevel2",
        )

        put_new_task_level1 = rail.RepliconServiceOperator(
            task_id='put_new_task_level1',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.role }}",
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.role }}",
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources": []
                }
            }
        )

        has_no_task_level2 = rail.IfOperator(
            task_id='has_no_task_level2',
            test="{{ result('get_all_project_tasks').tasklevel2_name | is_falsy }}",
            yes_task="put_new_task_level2",
            no_task="has_no_tasklevel3_name",
        )

        put_new_task_level2 = rail.RepliconServiceOperator(
            task_id='put_new_task_level2',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.service }}",
                        "parent": {
                            "uri": "{{ result('put_new_task_level1').uri }}",
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.service }}",
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources": []
                }
            }
        )

        has_no_tasklevel3_name = rail.IfOperator(
            task_id='has_no_tasklevel3_name',
            test="{{ result('get_all_project_tasks').tasklevel3_name | is_falsy }}",
            yes_task="put_new_task_level3",
            no_task="can_update_task_resource_assignment",
        )

        put_new_task_level3 = rail.RepliconServiceOperator(
            task_id='put_new_task_level3',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.description }}",
                        "parent": {
                            "uri": "{{ result('put_new_task_level2').uri }}",
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.description }}",
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources": []
                }
            }
        )

        can_update_task_resource_assignment = rail.IfOperator(
            task_id='can_update_task_resource_assignment',
            test=lambda: bool((rail.result('put_new_task_level3') or {}).get('uri') or (rail.result(
                'put_new_task_level2') or {}).get('uri') or (rail.result('put_new_task_level1') or {}).get('uri')),
            yes_task="update_task_resource_assignment",
            no_task="has_not_created_tasklevel2",
        )

        update_task_resource_assignment = rail.RepliconServiceOperator(
            task_id='update_task_resource_assignment',
            endpoint="/services/TaskService1.svc/UpdateResourceAssignment",
            data=lambda: {
                "resourceUri": rail.result('search_users')['uri'],
                "isAssigned": "true",
                "taskUri": (rail.result('put_new_task_level3') or {}).get('uri') or (rail.result('put_new_task_level2') or {}).get('uri') or (rail.result('put_new_task_level1') or {}).get('uri')
            }
        )

        has_not_created_tasklevel2 = rail.IfOperator(
            task_id='has_not_created_tasklevel2',
            test="{{ result('put_new_task_level2') | is_falsy }}",
            yes_task="has_notasklevel2_in_replicon",
            no_task="has_not_created_task_level3",
        )

        has_notasklevel2_in_replicon = rail.IfOperator(
            task_id='has_notasklevel2_in_replicon',
            test="{{ result('get_all_project_tasks').tasklevel2_name | is_falsy and dag_run.conf.service | is_truthy }}",
            yes_task="put_newtask_level2",
            no_task="has_notasklevel3_in_replicon",
        )

        put_newtask_level2 = rail.RepliconServiceOperator(
            task_id='put_newtask_level2',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.service }}",
                        "parent": {
                            "uri": "{{ result('get_all_project_tasks').tasklevel1_uri }}",
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.service }}",
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources": []
                }
            }
        )

        has_notasklevel3_in_replicon = rail.IfOperator(
            task_id='has_notasklevel3_in_replicon',
            test="{{ result('get_all_project_tasks').tasklevel3_name | is_falsy and dag_run.conf.description | is_truthy }}",
            yes_task="put_newtask_level3",
            no_task="can_update_task_resource_assignment2",
        )

        put_newtask_level3 = rail.RepliconServiceOperator(
            task_id='put_newtask_level3',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.description }}",
                        "parent": {
                            "uri": "{{ result('get_all_project_tasks').tasklevel2_uri or result('put_newtask_level2').uri }}",
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.description }}",
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources": []
                }
            }
        )

        can_update_task_resource_assignment2 = rail.IfOperator(
            task_id='can_update_task_resource_assignment2',
            test=lambda: bool((rail.result('put_newtask_level3') or {}).get(
                'uri') or (rail.result('put_newtask_level2') or {}).get('uri')),
            yes_task="update_task_resource_assignment2",
            no_task="has_not_created_task_level3",
        )

        update_task_resource_assignment2 = rail.RepliconServiceOperator(
            task_id='update_task_resource_assignment2',
            endpoint="/services/TaskService1.svc/UpdateResourceAssignment",
            data=lambda: {
                "resourceUri": rail.result('search_users')['uri'],
                "isAssigned": "true",
                "taskUri": (rail.result('put_newtask_level3') or {}).get('uri') or (rail.result('put_newtask_level2') or {}).get('uri')
            }
        )

        has_not_created_task_level3 = rail.IfOperator(
            task_id='has_not_created_task_level3',
            test="{{ result('put_new_task_level3') | is_falsy and result('put_newtask_level3') | is_falsy }}",
            yes_task="has_tasklevel3_name2",
            no_task="has_no_task_level_uri",
        )

        has_tasklevel3_name2 = rail.IfOperator(
            task_id='has_tasklevel3_name2',
            test="{{ result('get_all_project_tasks').tasklevel3_name | is_falsy and dag_run.conf.description | is_truthy }}",
            yes_task="put_task_level3_2",
            no_task="has_no_task_level_uri",
        )

        put_task_level3_2 = rail.RepliconServiceOperator(
            task_id='put_task_level3_2',
            endpoint="/services/ProjectService1.svc/PutTask",
            data={
                "project": {
                    "uri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": "{{dag_run.conf.description }}",
                        "parent": {
                            "uri": "{{ result('get_all_project_tasks').tasklevel2_uri }}",
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.description }}",
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "false",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                    "assignedResources": []
                }
            }
        )

        update_tasklevel3_resource_assignment = rail.RepliconServiceOperator(
            task_id='update_tasklevel3_resource_assignment',
            endpoint="/services/TaskService1.svc/UpdateResourceAssignment",
            data=lambda: {
                "resourceUri": rail.result('search_users')['uri'],
                "isAssigned": "true",
                "taskUri": (rail.result('put_task_level3_2') or {}).get('uri')
            }
        )

        has_no_task_level_uri = rail.IfOperator(
            task_id='has_no_task_level_uri',
            test="{{ result('get_all_project_tasks').tasklevel1_uri | is_falsy or  result('get_all_project_tasks').tasklevel2_uri | is_falsy or result('get_all_project_tasks').tasklevel3_uri | is_falsy }}",
            yes_task="get_all_project_task2",
            no_task="has_resourcescheduleservice_time",
        )

        get_all_project_task2 = rail.RepliconServiceOperator(
            task_id="get_all_project_task2",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                "parentUri": "{{ result('get_project_details')[0].projectDetails.uri }}"
            },
            data_handler=map_task
        )

        has_resourcescheduleservice_time = rail.IfOperator(
            task_id='has_resourcescheduleservice_time',
            test="{{ result('search_timeentry') | find_first_by_attr_and_get_attr('timeentryid',dag_run.conf.resourcescheduleserviceID | int | string) | is_truthy }}",
            yes_task="update_time_entry",
            no_task="add_new_time_entry",
        )

        def format_hours(decimal_hours):
            def frac(n):
                i = int(n)
                f = round((n - int(n)), 4)
                return (i, f)

            hours, _min = frac(decimal_hours)
            minutes, _sec = frac(_min*60)
            seconds, _ = frac(_sec*60)
            return {
                "minutes": minutes,
                "hours": hours,
                "seconds": seconds
            }

        def get_timeentry_param():
            conf = rail.get_current_context()['dag_run'].conf
            project_task = rail.result(get_all_project_task3.task_id) or rail.result(
                get_all_project_task2.task_id) or rail.result(get_all_project_tasks.task_id)
            task_uri = project_task['tasklevel3_uri'] or project_task['tasklevel2_uri'] or project_task['tasklevel1_uri']
            target_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                search_timeentry.task_id), 'timeentryid', str(int(conf['resourcescheduleserviceID'])), 'timeentryrevisiongroup')
            return {
                "unitOfWorkId": f"{uuid.uuid4()}.PutTimeEntryRevisionGroup",
                "timeEntryRevisionGroup": {
                    "user": {
                        "uri": rail.result('get_user_group')['userUri']
                    },
                    "timeAllocationTypeUris": ['urn:replicon:time-allocation-type:project'],
                    "entryDate": get_replicon_date(parse_date(conf['starttime'])),
                    "customMetadata": [
                        {
                            "value": {
                                "uri": "urn:replicon:project-specific-billing-rate",
                            },
                            "keyUri": "urn:replicon:time-entry-metadata-key:billing-rate"
                        },
                        {
                            "keyUri": "urn:replicon:time-entry-metadata-key:task",
                            "value": {
                                "uri": task_uri,
                            }
                        },
                        {
                            "keyUri": "urn:replicon:widget-ui-metadata-key:initial-row-number",
                            "value": {
                                "number": int(task_uri.split(":")[-1]) if int(task_uri.split(":")[-1]) > 100 else int(task_uri.split(":")[-1]) + 100,
                            }
                        }
                    ],

                    "extensionFieldValues": [
                        {
                            "textValue": int(conf['resourcescheduleserviceID']),
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result(get_timeentry_oef.task_id), 'displayText', "RSSID", 'uri'),
                            }
                        }
                    ],
                    "target": {
                        "uri": target_uri,
                    } if target_uri else null,
                    "interval": {
                        "hours": format_hours(float(conf['duration']))
                    }
                }
            }

        update_time_entry = rail.RepliconServiceOperator(
            task_id='update_time_entry',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=get_timeentry_param

        )

        add_new_time_entry = rail.RepliconServiceOperator(
            task_id='add_new_time_entry',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=get_timeentry_param
        )

        def map_task2(data):
            tasks = []

            def add_child_taks(child_tasks):
                for child_task in child_tasks:
                    tasks.append(child_task['task'])
                    if child_task['childTasks']:
                        add_child_taks(child_task['childTasks'])

            for item in data:
                tasks.append(item['task'])
                add_child_taks(item['childTasks'])

            return {
                "tasklevel1_name": " ".join(list(map(lambda x: x['name'], filter(lambda x: x['name'] == rail.get_current_context()['dag_run'].conf['description'], tasks)))),
                "tasklevel1_uri": " ".join(list(map(lambda x: x['uri'], filter(lambda x: x['name'] == rail.get_current_context()['dag_run'].conf['description'], tasks)))),
                "tasklevel2_name": "",
                "tasklevel2_uri":  "",
                "tasklevel3_name":  "",
                "tasklevel3_uri": "",
            }

        get_all_project_task3 = rail.RepliconServiceOperator(
            task_id="get_all_project_task3",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                "parentUri": "{{ result('get_project_details')[0].projectDetails.uri }}"
            },
            data_handler=map_task2
        )

        has_resourceschedule_serviceid2 = rail.IfOperator(
            task_id='has_resourceschedule_serviceid2',
            test="{{ result('search_timeentry') | find_first_by_attr_and_get_attr('timeentryid',dag_run.conf.resourcescheduleserviceID | int |string) | is_truthy }}",
            yes_task="update_time_entry2",
            no_task="add_new_time_entry2",
        )

        update_time_entry2 = rail.RepliconServiceOperator(
            task_id='update_time_entry2',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=get_timeentry_param

        )

        add_new_time_entry2 = rail.RepliconServiceOperator(
            task_id='add_new_time_entry2',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=get_timeentry_param
        )

        add_success_log = rail.WriteLogOperator(
            task_id='add_success_log',
            log="{{ result('create_log') }}",
            message="The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has been completed successfully.",
            severity='Success',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'message': "The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has been completed successfully.",
                'status': 'Success',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}}  has not been completed. Error: {{ get_error_message() }}',
            properties={
                "fd_status": "{{dag_run.conf.fd_status}}", "mill_mpc": "{{dag_run.conf.mill_mpc}}",
                "projectnumber|title": "{{dag_run.conf.projectnumber}}|{{dag_run.conf.title}}",
                "role|service": "{{dag_run.conf.role}}|{{dag_run.conf.service}}",
                "description": "{{dag_run.conf.description}}",
                "starttime|endtime": "{{dag_run.conf.starttime}}|{{dag_run.conf.endtime}}",
                "duration|resourcename|resourcescheduleserviceid": "{{dag_run.conf.duration}}|{{dag_run.conf.resourcename}}|{{dag_run.conf.resourcescheduleserviceID}}",
                'status': 'Error',
                'message': 'The Schedule data transfer from CETA to Replicon with job reference {{dag_run_ecid()}} has not been completed. Error: {{ get_error_message() }}',

            },
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            trigger_rule='all_done',
            python_callable=lambda: rail.load_all_records(
                rail.result('create_log'))
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors >> format_logs >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> has_invalid_fd_status

        has_invalid_fd_status >> rail.Label(
            'no') >> validate_record >> has_invalid_record
        has_invalid_fd_status >> rail.Label('yes') >> finish

        has_invalid_record >> rail.Label(
            'Yes') >> add_invalid_record_log_entry >> finish
        has_invalid_record >> rail.Label(
            'No') >> has_resourcename

        has_resourcename >> rail.Label(
            'Yes') >> search_users >> has_invalid_user
        has_resourcename >> rail.Label(
            'no') >> get_timeentry_oef

        has_invalid_user >> rail.Label('Yes') >> add_invalid_user_log >> finish
        has_invalid_user >> rail.Label(
            'no') >> has_invalid_daterange

        has_invalid_daterange >> rail.Label(
            'yes') >> add_invalid_daterange_log >> finish
        has_invalid_daterange >> rail.Label('no') >> get_timeentry_oef

        get_timeentry_oef >> get_project_details >> \
            get_user_group >> get_timesheet >> has_no_timesheet

        has_no_timesheet >> rail.Label('Yes') >> add_no_timesheet_log >> finish
        has_no_timesheet >> rail.Label('no') >> get_timesheet_details >> get_timeentry_revision_columns >>\
            get_timeentry_revision_filters >> has_no_usergroup

        has_no_usergroup >> rail.Label('Yes') >> add_no_usergroup_log >> finish
        has_no_usergroup >> rail.Label(
            'no') >> search_timeentry >> has_cancelled_fd_status

        has_cancelled_fd_status >> rail.Label(
            'Yes') >> has_resource_scheduleserviceID
        has_cancelled_fd_status >> rail.Label('no') >> has_no_timesheet_uri

        has_resource_scheduleserviceID >> rail.Label(
            'Yes') >> has_no_open_timesheet
        has_resource_scheduleserviceID >> rail.Label(
            'no') >> add_noentry_log >> finish

        has_no_open_timesheet >> rail.Label(
            'Yes') >> add_invalid_timesheet_status >> finish
        has_no_open_timesheet >> rail.Label(
            'no') >> delete_timentry_revision >> add_delete_log >> finish

        has_no_timesheet_uri >> rail.Label(
            'Yes') >> add_no_timesheeturi_log >> finish
        has_no_timesheet_uri >> rail.Label('no') >> has_invalid_timesheetstatus

        has_invalid_timesheetstatus >> rail.Label(
            'Yes') >> add_invalid_timesheet_status_log >> finish
        has_invalid_timesheetstatus >> rail.Label(
            'no') >> has_projectnotpresent

        has_projectnotpresent >> rail.Label(
            'Yes') >> add_projectnotfound_log >> finish
        has_projectnotpresent >> rail.Label(
            'no') >> has_uri_projecturiispresent

        has_uri_projecturiispresent >> rail.Label('Yes') >> has_no_absense
        has_uri_projecturiispresent >> rail.Label(
            'no') >> add_projectnotfound_log

        has_no_absense >> rail.Label(
            'Yes') >> get_all_project_tasks >> has_no_tasklevel1_name
        has_no_absense >> rail.Label('no') >> get_all_project_task3

        has_no_tasklevel1_name >> rail.Label(
            'Yes') >> put_new_task_level1 >> has_no_task_level2
        has_no_tasklevel1_name >> rail.Label(
            'no') >> has_not_created_tasklevel2

        has_no_task_level2 >> rail.Label(
            'Yes') >> put_new_task_level2 >> has_no_tasklevel3_name
        has_no_task_level2 >> rail.Label('no') >> has_no_tasklevel3_name

        has_no_tasklevel3_name >> rail.Label(
            'yes') >> put_new_task_level3 >> update_task_resource_assignment >> has_not_created_tasklevel2
        has_no_tasklevel3_name >> rail.Label(
            'no') >> can_update_task_resource_assignment

        can_update_task_resource_assignment >> rail.Label(
            'yes') >> update_task_resource_assignment >> has_not_created_tasklevel2
        can_update_task_resource_assignment >> rail.Label(
            'no') >> has_not_created_tasklevel2

        has_not_created_tasklevel2 >> rail.Label(
            'Yes') >> has_notasklevel2_in_replicon
        has_not_created_tasklevel2 >> rail.Label(
            'no') >> has_not_created_task_level3

        has_notasklevel2_in_replicon >> rail.Label(
            'Yes') >> put_newtask_level2 >> has_notasklevel3_in_replicon
        has_notasklevel2_in_replicon >> rail.Label(
            'no') >> has_notasklevel3_in_replicon

        has_notasklevel3_in_replicon >> rail.Label(
            'Yes') >> put_newtask_level3 >> can_update_task_resource_assignment2
        has_notasklevel3_in_replicon >> rail.Label(
            'no') >> can_update_task_resource_assignment2

        can_update_task_resource_assignment2 >> rail.Label(
            'yes') >> update_task_resource_assignment2 >> has_not_created_task_level3
        can_update_task_resource_assignment2 >> rail.Label(
            'no') >> has_not_created_task_level3

        has_not_created_task_level3 >> rail.Label(
            'Yes') >> has_tasklevel3_name2
        has_not_created_task_level3 >> rail.Label(
            'no') >> has_no_task_level_uri

        has_tasklevel3_name2 >> rail.Label(
            'Yes') >> put_task_level3_2 >> update_tasklevel3_resource_assignment >> has_no_task_level_uri
        has_tasklevel3_name2 >> rail.Label('no') >> has_no_task_level_uri

        has_no_task_level_uri >> rail.Label(
            'Yes') >> get_all_project_task2 >> has_resourcescheduleservice_time
        has_no_task_level_uri >> rail.Label(
            'no') >> has_resourcescheduleservice_time

        has_resourcescheduleservice_time >> rail.Label(
            'Yes') >> update_time_entry >> add_success_log
        has_resourcescheduleservice_time >> rail.Label(
            'no') >> add_new_time_entry >> add_success_log

        get_all_project_task3 >> has_resourceschedule_serviceid2
        has_resourceschedule_serviceid2 >> rail.Label(
            'Yes') >> update_time_entry2 >> add_success_log >> finish
        has_resourceschedule_serviceid2 >> rail.Label(
            'no') >> add_new_time_entry2 >> add_success_log >> finish

        finish >> catch_and_log_errors >> format_logs >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
