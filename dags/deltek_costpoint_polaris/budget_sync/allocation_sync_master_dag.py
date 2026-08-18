from datetime import timedelta, timezone
from datetime import datetime
import math
from airflow.models import Variable, DagRun
import rail


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_allocation_sync_master_{config.instance}',
        description=f'deltek_costpoint_allocation_sync_master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def get_dagruns_to_process(lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):
            current_time = datetime.now(timezone.utc)
            lookup_timestamp_value = Variable.get(
                lookup_log_timestamp_var, default_var=None)
            query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
                current_time - timedelta(hours=lookup_log_timestamp_hours))
            dag_runs = []
            execution_dates = []
            for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
                execution_dates.append(run.execution_date)
                dag_runs.append(run.id)
            if execution_dates:
                max_execution_date = max(execution_dates)
                Variable.set(lookup_log_timestamp_var,
                             (max_execution_date + timedelta(seconds=1)).isoformat())
            return dag_runs

        get_allocation_dagruns_to_process = rail.PythonOperator(
            task_id='get_allocation_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.lookup_allocation_timestamp_var,
                     config.lookup_allocation_timestamp_hours,
                     f'deltek_costpoint_collecting_project_allocation_{config.instance}']
        )

        is_allocation_dagruns_present = rail.IfOperator(
            task_id='is_allocation_dagruns_present',
            test="{{ result('get_allocation_dagruns_to_process') | length > 0 }}",
            yes_task='get_project_allocations',
            no_task='delete_this_dagrun'
        )

        get_project_allocations = rail.GatherResultsFromDagRunsOperator(
            task_id='get_project_allocations',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_allocation_dagruns_to_process') }}",
            dagrun_task_id='collecting_project_data',
            flatten=True
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('get_project_allocations') | length > 0 }}",
            yes_task='get_sub_period_info',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        def get_project_codes():
            return [
                {
                    "project_id": "01001",
                    "user_id": "DEM"
                }
            ]

        def get_subperiod_details(subperiod_rows):
            subperiod_details = []
            if subperiod_rows:
                for subperiod in subperiod_rows[0]['row']['children']:
                    if subperiod['row']['data'].get('END_DATE'):
                        subperiod_details.append({
                            "end_date": subperiod['row']['data'].get('END_DATE'),
                            "fy_cd": subperiod['row']['data'].get('FY_CD'),
                            "pd_no": subperiod['row']['data'].get('PD_NO'),
                            "sub_pd_no": subperiod['row']['data'].get('SUB_PD_NO')
                        })

            return subperiod_details

        get_sub_period_info = rail.DeltekCostPointServiceOperator(
            task_id='get_sub_period_info',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_periods",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "BNP_BAMMAM8",
                                "conditions": [
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: get_subperiod_details(
                data['document']['rows']),
        )

        declare_list_1 = rail.SetVariableOperator(
            task_id='declare_list_1',
            append=False,
            name='project_allocation_data',
            value=[]
        )

        def get_modified_allocated_project(type_name):
            modified_allocated_data = rail.result('get_project_allocations')
            project_allocations = list(filter(
                lambda x: x[type_name], modified_allocated_data))
            modified_allocations = []
            for allocation_detail in project_allocations:
                if not any(list(filter(lambda x: x[type_name]['uri'] ==
                                       allocation_detail[type_name]['uri'], modified_allocations))):
                    modified_allocations.append(allocation_detail)
            return modified_allocations

        foreach_get_projects_allocations = rail.ForEachOperator(
            task_id='foreach_get_projects_allocations',
            items=lambda: get_modified_allocated_project('project'),
            start_task='get_project_allocation_from_replicon',
            end_task='foreach_get_projects_allocations_end'
        )

        get_project_allocation_from_replicon = rail.RepliconServiceOperator(
            task_id='get_project_allocation_from_replicon',
            endpoint="graphql",
            app="polaris",
            data={
                    "variables": {
                        "projectUri": '''{{ result('foreach_get_projects_allocations').project.uri }}''',
                        "limit": 1000,
                        "allocationStatusList": ["COMMITTED"],
                        "showTimeOff": True,
                        "chartDateRange":
                        {
                            "startDate": '''{{ result('foreach_get_projects_allocations').project.project_start_date }}''',
                            "endDate": '''{{ result('foreach_get_projects_allocations').project.project_end_date }}'''
                        },
                        "showHolidays": True
                    },
                "query": '''query Eager_GetAllocations($projectUri: String, $allocationStatusList: [ResourceAllocationStatus], $limit: Int, $cursor: String, $sort: ResourceAllocationSort, $showTimeOff: Boolean!, $chartDateRange: DateRangeInput, $showHolidays: Boolean!, $filter: ResourceAllocationFilter) {
                resourceAllocations(
                    projectUri: $projectUri
                    limit: $limit
                    cursor: $cursor
                    allocationStatusList: $allocationStatusList
                    sort: $sort
                    filter: $filter
                ) {
                    resourceAllocations {
                    ...SpecificResourceAllocation
                    role {
                        uri
                        id
                        displayText
                        __typename
                    } project {
                        uri
                        code
                        startDate
                        endDate
                        __typename
                    }
                    user {
                        ...SpecificResourceAllocationUser
                        ...SpecificResourceAllocationTimeOff
                        ...SpecificResourceAllocationHolidays
                        scheduleDurationByDay(dateRange: $chartDateRange) {
                        date
                        hours
                        __typename
                        }
                        __typename
                    }
                    ...AllocationTotalsFragment
                    __typename
                    }
                    nextPageCursor
                    __typename
                }
                }

                fragment SpecificResourceAllocation on ResourceAllocation {
                id
                projectUri
                resourceRequestId
                user {
                    ...SpecificResourceAllocationUser
                    __typename
                }
                allocationStatus
                totalHours
                scheduleRules {
                    dateRange {
                    startDate
                    endDate
                    __typename
                    }
                    do
                    __typename
                }
                startDate
                endDate
                load
                isAdjustedLoading
                requestedRoleUri
                __typename
                }

                fragment SpecificResourceAllocationUser on ResourceAllocationUser {
                userAllocationId
                userUri
                user {
                    slug
                    displayText
                    uri
                    loginName
                    __typename
                }
                __typename
                }
                fragment SpecificResourceAllocationTimeOff on ResourceAllocationUser {
                timeoffs(dateRange: $chartDateRange) @include(if: $showTimeOff) {
                    dateRange {
                    startDate
                    endDate
                    __typename
                    }
                    timeOffType {
                    displayText
                    __typename
                    }
                    hours
                    days
                    entries {
                    date
                    hours
                    days
                    __typename
                    }
                    __typename
                }
                __typename
                }

                fragment SpecificResourceAllocationHolidays on ResourceAllocationUser {
                holidays(dateRange: $chartDateRange) @include(if: $showHolidays) {
                    date
                    uri
                    name
                    duration {
                    hours
                    minutes
                    seconds
                    __typename
                    }
                    effectiveDuration {
                    hours
                    minutes
                    seconds
                    __typename
                    }
                    __typename
                }
                __typename
                }

                fragment AllocationTotalsFragment on ResourceAllocation {
                totalHours
                __typename}'''
            },
        )

        def datetime_to_excel_serial_date(date):
            # Excel's base date is December 30, 1899
            excel_base_date = datetime(1899, 12, 30)
            delta = date - excel_base_date
            excel_serial_date = delta.days + delta.seconds / \
                (24 * 60 * 60)  # Include fraction of a day
            return math.floor(excel_serial_date)

        def get_sub_period_end_date(allocation_date):
            for period_info in rail.result('get_sub_period_info'):
                period_sub_end_date = datetime.strptime(
                    period_info['end_date'], config.costpoint_date_format) \
                    if period_info['end_date'] else None
                if allocation_date <= period_sub_end_date:
                    return datetime_to_excel_serial_date(period_sub_end_date)
            return None

        def get_daterange(start_date, end_date, exclude_weekdays=None):
            exclude_weekdays = exclude_weekdays or []
            weekday_map = {'mo': 0, 'tu': 1, 'we': 2, 'th': 3, 'fr': 4, 'sa': 5, 'su': 6}
            excluded = {weekday_map[d] for d in exclude_weekdays if d in weekday_map}
            for n in range(int((end_date - start_date).days) + 1):
                date = start_date + timedelta(n)
                if date.weekday() not in excluded:
                    yield date

        def get_project_budget_records():
            project_allocation_details = rail.result(
                'get_project_allocation_from_replicon')
            proj_alloc_info_arr = project_allocation_details[
                'data']['resourceAllocations']['resourceAllocations']
            budget_details = []
            for proj_allocation in proj_alloc_info_arr:
                scheduleRules = proj_allocation['scheduleRules']
                for sch_hours in scheduleRules:
                    allocation_start_date = datetime.strptime(
                        sch_hours['dateRange']['startDate'], config.polaris_date_format) if sch_hours['dateRange']['startDate'] else None
                    allocation_end_date = datetime.strptime(
                        sch_hours['dateRange']['endDate'], config.polaris_date_format) if sch_hours['dateRange']['endDate'] else None
                    for allocation_date in get_daterange(
                        allocation_start_date,
                        allocation_end_date,
                        exclude_weekdays=sch_hours['do'].get('excludeWeekdays', [])
                    ):
                        sub_period_end_date = get_sub_period_end_date(
                            allocation_date)
                        if sch_hours['do']['setHours'] > 0:
                            expected_hours = sch_hours['do']['load'] * \
                                (sch_hours['do']['setHours'] / 100)
                            same_end_date = list(
                                filter(lambda x: x['projectenddate'] == sub_period_end_date
                                       and x['id'] == proj_allocation['user']['user']['loginName'], budget_details))
                            if same_end_date:
                                same_end_date[0]['hours'] += expected_hours
                            else:
                                budget_details.append({
                                    "projectid": proj_allocation['project']['code'],
                                    "projectenddate": sub_period_end_date,
                                    "idtype": "Employee",
                                    "userid": proj_allocation['user']['user']['uri'],
                                    "id": proj_allocation['user']['user']['loginName'],
                                    "poolorgid": "DEFAULT",
                                    "accountid": "DEFAULT",
                                    "plc": "DEFAULT",
                                    "hours": expected_hours,
                                })

            return budget_details

        insert_project_allocation_into_list = rail.SetVariableOperator(
            task_id='insert_project_allocation_into_list',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value=lambda: {
                "project": rail.result('foreach_get_projects_allocations')['project']['uri'],
                "allocation": get_project_budget_records()
            }
        )

        foreach_get_projects_allocations_end = rail.EmptyOperator(
            task_id='foreach_get_projects_allocations_end',
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='task_allocation_data',
            value=[]
        )

        foreach_get_task_allocations = rail.ForEachOperator(
            task_id='foreach_get_task_allocations',
            items=lambda: get_modified_allocated_project('task'),
            start_task='get_task_allocation_from_replicon',
            end_task='foreach_get_task_allocations_end'
        )

        def get_task_budget_records():
            task_allocation_details = rail.result(
                'get_task_allocation_from_replicon')
            task_alloc_info_arr = task_allocation_details['data'][
                'task']['resourceAllocations']
            allocation_start_date = datetime.strptime(
                task_allocation_details['data']['task']['startDate'], config.polaris_date_format) if task_allocation_details['data']['task']['startDate'] else None
            allocation_end_date = datetime.strptime(
                task_allocation_details['data']['task']['endDate'], config.polaris_date_format) if task_allocation_details['data']['task']['endDate'] else None
            numberof_days = (allocation_end_date -
                             allocation_start_date).days + 1
            budget_details = []
            for task_allocation in task_alloc_info_arr:
                allocated_hours = task_allocation['allocatedHours']
                if allocated_hours:
                    allocated_hours_days = allocated_hours / numberof_days
                    for allocation_date in get_daterange(allocation_start_date, allocation_end_date):
                        sub_period_end_date = get_sub_period_end_date(
                            allocation_date)
                        same_end_date = list(
                            filter(lambda x: x['projectenddate'] == sub_period_end_date
                                   and x['id'].upper() == task_allocation['resource']['slug'].upper(), budget_details))
                        if same_end_date:
                            same_end_date[0]['hours'] += allocated_hours_days
                        else:
                            budget_details.append({
                                "projectid": task_allocation_details['data']['task']['code'],
                                "projectenddate": sub_period_end_date,
                                "idtype": "Employee",
                                "userid": task_allocation['resource']['id'],
                                "id": task_allocation['resource']['slug'].upper(),
                                "poolorgid": "DEFAULT",
                                "accountid": "DEFAULT",
                                "plc": "DEFAULT",
                                "hours": allocated_hours_days,
                            })

            return budget_details

        get_task_allocation_from_replicon = rail.RepliconServiceOperator(
            task_id='get_task_allocation_from_replicon',
            endpoint="graphql",
            app="polaris",
            data={"variables":
                  {
                    "taskId": '''{{ result('foreach_get_task_allocations').task.uri }}''',
                    "page": 1,
                    "pageSize": 10000
                  },
                  "query": '''query getTaskResourceAllocations($taskId: String!, $page: Int!, $pageSize: Int!) {
                    task(taskId: $taskId) {
                        code
                    
                    startDate
                            endDate
                        id
                        resourceAllocations(page: $page, pageSize: $pageSize) {
                        resource {
                            id
                            slug
                            displayText
                            __typename
                        }
                        allocatedHours
                        __typename
                        }
                        __typename
                    }
                    }'''
                  },
        )

        insert_task_allocation_into_list = rail.SetVariableOperator(
            task_id='insert_task_allocation_into_list',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value=lambda: {
                "project": rail.result('foreach_get_task_allocations')['task']['uri'],
                "allocation": get_task_budget_records()
            }
        )

        foreach_get_task_allocations_end = rail.EmptyOperator(
            task_id='foreach_get_task_allocations_end',
        )

        def get_budget_data():
            project_allocations_data = rail.get_dag_run_var(
                rail.result("declare_list_1")['name'])
            project_data = []
            if project_allocations_data:
                for project_allocations in project_allocations_data:
                    project_data = project_data + \
                        project_allocations['allocation']
            task_data = []
            task_allocations_data = rail.get_dag_run_var(
                rail.result("declare_list_2")['name'])
            if task_allocations_data:
                for task_allocations in task_allocations_data:
                    task_data = task_data + task_allocations['allocation']
            return project_data + task_data

        collecting_allocation_data = rail.PythonOperator(
            task_id='collecting_allocation_data',
            python_callable=lambda: get_budget_data()
        )

        if_allocations_present = rail.IfOperator(
            task_id='if_allocations_present',
            test='''{{ result('collecting_allocation_data') | length > 0 }}''',
            yes_task="get_allocated_user_details",
            no_task="finish",
        )

        def get_user_idtypes(response):
            user_id_type_details = []
            for user_details in response:
                user_id_type_details.append({
                    "useruri": user_details['userDetails']['uri'],
                    "idtype": rail.find_first_by_attr_and_get_attr(
                        user_details['userDetails']['extensionFieldValues'], 'definition.displayText', 'Id Type', 'tag.displayText')
                })

            return user_id_type_details

        get_allocated_user_details = rail.RepliconServiceOperator(
            task_id='get_allocated_user_details',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda: {
                    "users": [{
                        "uri": x
                    } for x in set([user['userid'] for user in rail.result('collecting_allocation_data')])],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=get_user_idtypes
        )

        def get_allocations_data():
            allocation_data = []
            all_allocations_data = rail.result('collecting_allocation_data')
            allocated_user_details = rail.result('get_allocated_user_details')
            for allocation in all_allocations_data:
                allocation_data.append({
                    "projectid": allocation['projectid'],
                    "projectenddate": allocation['projectenddate'],
                    "idtype": rail.find_first_by_attr_and_get_attr(
                        allocated_user_details, 'useruri', allocation['userid'], 'idtype'),
                    "userid": allocation['userid'],
                    "id": allocation['id'],
                    "poolorgid": allocation['poolorgid'],
                    "accountid": allocation['accountid'],
                    "plc": allocation['plc'],
                    "hours": allocation['hours'],
                })
            return allocation_data

        preparing_allocation_data = rail.PythonOperator(
            task_id='preparing_allocation_data',
            python_callable=lambda: get_allocations_data()
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='templates/output/output_template.xml',
            dataset=lambda: rail.result('preparing_allocation_data'),
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_xml_file')}}",
            output_file_name='Budget_{{ dag_run_ecid() | replace(":", "-") }}.xml',
            expires_in_seconds=7*24*60*60,
        )

        send_budget_mail = rail.EmailOperator(
            task_id='send_budget_mail',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''Deltek Costpoint Budget Data''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint Budget file is ready. Please find the  link below to download the file.
            <br /> <br /> <a href="{{ result('generate_download_link') }}">Download Budget file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        upload_xml_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_sftp',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.processing_file_directory +
            '/' +
            'Budget_{{ dag_run_ecid() | replace(":", "-") }}.xml',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        get_allocation_dagruns_to_process >> is_allocation_dagruns_present
        is_allocation_dagruns_present >> rail.Label('No') >> delete_this_dagrun
        is_allocation_dagruns_present >> rail.Label(
            'Yes') >> get_project_allocations >> has_any_data
        has_any_data >> rail.Label('No') >> delete_this_dagrun
        has_any_data >> rail.Label('Yes') >> \
            get_sub_period_info >> declare_list_1 >> foreach_get_projects_allocations >> get_project_allocation_from_replicon >> \
            insert_project_allocation_into_list >> foreach_get_projects_allocations_end
        foreach_get_projects_allocations >> foreach_get_projects_allocations_end >> declare_list_2 >> \
            foreach_get_task_allocations >> get_task_allocation_from_replicon >> insert_task_allocation_into_list >> \
            foreach_get_task_allocations_end
        foreach_get_task_allocations >> foreach_get_task_allocations_end >> collecting_allocation_data >> \
            if_allocations_present
        if_allocations_present >> rail.Label('No') >> finish
        if_allocations_present >> rail.Label('Yes') >> get_allocated_user_details >> preparing_allocation_data >> write_xml_file >> \
            generate_download_link >> send_budget_mail >> upload_xml_to_sftp >> finish

    return dag


rail.for_each_instance(create_dag)
