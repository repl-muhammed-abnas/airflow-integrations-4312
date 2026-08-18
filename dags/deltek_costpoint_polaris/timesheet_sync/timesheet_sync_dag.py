from datetime import datetime, timedelta
from airflow.models import Variable
import rail
# pylint:disable=undefined-loop-variable
# pylint:disable=inconsistent-return-statements
# pylint:disable=too-many-arguments
# pylint:disable=too-many-nested-blocks
# pylint:disable=too-many-statements
# Dummy
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timesheet_sync_{config.instance}',
        description=f'deltek_costpoint_timesheet_sync_poc_{config.instance}',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.cp_rep_webhook_secret)
        ],
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_replicon_timesheet'
        )

        is_sync_time_off_bookings = rail.IfOperator(
            task_id='is_sync_time_off_bookings',
            test=lambda: config.is_sync_time_off_bookings.lower() == 'true',
            yes_task='get_replicon_timeoffs',
            no_task='is_time_entry_against_project'
        )

        is_use_task_allocation = rail.IfOperator(
            task_id='is_use_task_allocation',
            test=lambda: config.use_task_based_allocation == True,
            yes_task='get_replicon_task_allocations',
            no_task='is_sync_to_open_period'
        )

        is_sync_to_open_period = rail.IfOperator(
            task_id='is_sync_to_open_period',
            test=lambda: getattr(config, 'sync_to_open_period', False) == True,
            yes_task='get_open_subperiods',
            no_task='get_existing_deltek_timesheet'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_replicon_timesheet',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_open_subperiods = rail.DeltekCostPointServiceOperator(
            task_id='get_open_subperiods',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_sub_periods",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "GLMSUBPD_SUBPD",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "SUB_PD_DISP_END_DT",
                                                "relation": "gt=",
                                                "value": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate'])
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                    {
                                        "rsWhere": {
                                            "rsId": "GLMSUBPD_SUBPDJNLSTATUS_CTW",
                                            "conditions": [],
                                            "children": []
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_costpoint_timesheet_periods = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_timesheet_periods',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_ts_periods",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMTSPD_TSPD_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "TS_PD_CD",
                                                "relation": "=",
                                                "value": rail.result('get_replicon_timesheet_period_details')['description']
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                    {
                                        "rsWhere": {
                                            "rsId": "LDMTSPD_TSPDSCH_DTL",
                                            "conditions": [],
                                            "children": []
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_existing_deltek_timesheet = rail.DeltekCostPointServiceOperator(
            task_id='get_existing_deltek_timesheet',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_ldmtime",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMTIME_TSHDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "EMPL_ID",
                                                "relation": "=",
                                                "value": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId']
                                            },
                                            {
                                                "name": "TS_DT",
                                                "relation": "=",
                                                "value": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate'])
                                            }

                                        ]
                                    },
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "EMPL_ID",
                                                "relation": "=",
                                                "value": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId']
                                            },
                                            {
                                                "name": "TH___CORRECTING_REF_DT",
                                                "relation": "=",
                                                "value": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate'])
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_current_timesheet_period = rail.PythonOperator(
            task_id='get_current_timesheet_period',
            python_callable=lambda: get_current_costpoint_timesheet_period(
                rail.result('get_costpoint_timesheet_periods'), rail.result('get_replicon_timesheet'))
        )

        get_existing_deltek_timesheet_open_period = rail.DeltekCostPointServiceOperator(
            task_id='get_existing_deltek_timesheet_open_period',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_ldmtime",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMTIME_TSHDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "EMPL_ID",
                                                "relation": "=",
                                                "value": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId']
                                            },
                                            {
                                                "name": "TS_DT",
                                                "relation": "=",
                                                "value": rail.result('get_current_timesheet_period')['END_DT']
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_replicon_timesheet = rail.RepliconServiceOperator(
            task_id='get_replicon_timesheet',
            endpoint="/services/timesheetservice1.svc/GetTimesheetDetails",
            data={
                'timesheetUri': '{{ dag_run.conf.webhook.data.timesheet.uri }}'
            }
        )

        get_replicon_timeoffs = rail.RepliconServiceOperator(
            task_id='get_replicon_timeoffs',
            endpoint="/services/timesheetservice1.svc/GetAllOverlappingTimeOffForTimesheet2",
            data={
                'timesheetUri': '{{ dag_run.conf.webhook.data.timesheet.uri }}'
            }
        )

        get_replicon_time_off_type_details = rail.RepliconServiceOperator(
            task_id='get_replicon_time_off_type_details',
            endpoint="/services/timeoffservice1.svc/BulkGetTimeOffTypeDetails",
            data=lambda :{
                "timeOffTypeUris": get_timeoff_type_uris(rail.result('get_replicon_timeoffs'))
            }
        )

        get_replicon_time_entries = rail.RepliconServiceOperator(
            task_id='get_replicon_time_entries',
            endpoint='/services/timeEntryrevisiongroupservice1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange',
            data=lambda: {
                "user": {
                    "uri": rail.result('get_replicon_timesheet')['owner']['uri']
                },
                "dateRange": {
                    "startDate": rail.result('get_replicon_timesheet')['dateRange']['startDate'],
                    "endDate": rail.result('get_replicon_timesheet')['dateRange']['endDate']
                }
            }
        )

        get_replicon_user_details = rail.RepliconServiceOperator(
            task_id='get_replicon_user_details',
            endpoint="/services/importservice1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [{"uri": rail.result('get_replicon_timesheet')['owner']['uri']}]
            }
        )

        get_replicon_timesheet_period_details = rail.RepliconServiceOperator(
            task_id='get_replicon_timesheet_period_details',
            endpoint="/services/TimesheetPeriodService2.svc/GetTimesheetPeriodDetails",
            data=lambda: {
                "timesheetPeriod": {
                    "uri": get_timesheet_period_uri(rail.result('get_replicon_user_details')[0], rail.result('get_replicon_timesheet')),
                    "name": null
                }
            }
        )

        is_costpoint_user = rail.IfOperator(
            task_id='is_costpoint_user',
            test=lambda: is_source_costpoint(
                rail.result('get_replicon_user_details')[0]),
            yes_task='get_replicon_time_entries',
            no_task='catch_error'
        )

        get_user_role = rail.RepliconServiceOperator(
            task_id='get_user_role',
            endpoint="/services/ResourceService1.svc/GetProjectRoleAssignmentScheduleForUser",
            data=lambda: {
                "userUri": rail.result('get_replicon_timesheet')['owner']['uri']
            }
        )

        get_replicon_task_details = rail.RepliconServiceOperator(
            task_id='get_replicon_task_details',
            endpoint="/services/taskservice1.svc/BulkGetTaskDetails",
            data=lambda: {
                "taskUris": get_task_uris(rail.result('get_replicon_time_entries'))
            }
        )

        get_replicon_project_details = rail.RepliconServiceOperator(
            task_id='get_replicon_project_details',
            endpoint='/services/projectservice1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": get_project_uris(rail.result('get_replicon_task_details'))
            }
        )

        get_division_details = rail.RepliconServiceOperator(
            task_id='get_division_details',
            endpoint='/services/divisionservice1.svc/BulkGetDivisionDetails',
            data=lambda: {
                "divisionUris": get_division_uris(rail.result('get_replicon_project_details'))
            }
        )

        get_replicon_role_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_replicon_role_details',
            items=lambda: get_role_uris(
                rail.result('get_replicon_task_details'), rail.result('get_replicon_time_entries')),
            endpoint="/services/ProjectRoleService1.svc/GetRoleDetails",
            data={
                "projectRoleUri": "{{ item }}",
                "asOfDate": null
            }
        )

        get_oef_tag_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_oef_tag_details',
            items=lambda: get_oef_tag_uris(
                rail.result('get_replicon_time_entries')),
            endpoint="/services/ObjectExtensionTagService1.svc/GetObjectExtensionTagDetails",
            data={
                "objectExtensionTagUri": "{{ item }}"
            }
        )

        #todo complete this
        get_replicon_task_allocations = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_replicon_task_allocations',
            items=lambda: get_task_uris(rail.result('get_replicon_time_entries')),
            endpoint='/services/TaskService1.svc/GetPageOfTaskResourceEstimates',
            data={
                "page": "1",
                "pageSize": "1000",
                "taskTarget": {
                    "uri": "{{item}}",
                }
            })

        get_time_entries_to_sync = rail.PythonOperator(
            task_id='get_time_entries_to_sync',
            python_callable=lambda:
            {
                "document": {
                    "id": "polaris_imp_ldmtime",
                    "rows": [
                        {
                            "row": {
                                "rsId": "LDMTIME_TSHDR",
                                "tranType": "INSERT",
                                "data": {
                                    "EMPL_ID": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId'],
                                    "FY_CD": get_period_info()['FY_CD'],
                                    "OTH_HRS": get_other_hours(rail.result('get_replicon_time_entries'), rail.result('get_replicon_pay_codes'),
                                                               rail.result('get_oef_tag_details'), rail.result('get_replicon_timeoffs')),
                                    "PD_NO": get_period_info()['PD_NO'],
                                    "REG_HRS": get_reg_hours(rail.result('get_replicon_time_entries'), rail.result('get_replicon_pay_codes'),
                                                             rail.result('get_oef_tag_details'), rail.result('get_replicon_timeoffs')),
                                    "SUB_PD_NO": get_period_info()['SUB_PD_NO'],
                                    "S_TS_TYPE_CD": get_timesheet_type_code_by_period(),
                                    "TH___AUTO_ADJ_PCT_RT": 1,
                                    "TS_DT": get_timesheet_date(rail.result('get_replicon_timesheet')),
                                    "TS_HDR_SEQ_NO": get_timesheet_header_seq(rail.result('get_existing_deltek_timesheet')[0])
                                },
                                "children": get_children(rail.result('get_replicon_time_entries'), rail.result('get_replicon_task_details'),
                                                         rail.result('get_replicon_pay_codes'), rail.result(
                                                             'get_replicon_role_details'),
                                                         rail.result('get_replicon_account_details'), rail.result(
                                                             'get_replicon_project_details'),
                                                         rail.result('get_division_details'), rail.result('get_oef_tag_details'),
                                                         rail.result('get_replicon_user_details')[0]['userDetails']['uri'],
                                                         rail.result('get_replicon_timeoffs'),
                                                         rail.result('get_replicon_time_off_type_details'),
                                                         rail.result('get_replicon_task_allocations')
                                                         )
                            }
                        }
                    ]
                }
            }

        )

        push_time_to_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='push_time_to_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda:
            {
                "document": {
                    "id": "polaris_imp_ldmtime",
                    "rows": [
                        {
                            "row": {
                                "rsId": "LDMTIME_TSHDR",
                                "tranType": "INSERT",
                                "data": {
                                    "EMPL_ID": rail.result('get_replicon_user_details')[0]['userDetails']['employeeId'],
                                    "FY_CD": get_period_info()['FY_CD'],
                                    "OTH_HRS": get_other_hours(rail.result('get_replicon_time_entries'), rail.result('get_replicon_pay_codes'),
                                                               rail.result('get_oef_tag_details'), rail.result('get_replicon_timeoffs')),
                                    "PD_NO": get_period_info()['PD_NO'],
                                    "REG_HRS": get_reg_hours(rail.result('get_replicon_time_entries'), rail.result('get_replicon_pay_codes'),
                                                             rail.result('get_oef_tag_details'), rail.result('get_replicon_timeoffs')),
                                    "SUB_PD_NO": get_period_info()['SUB_PD_NO'],
                                    "S_TS_TYPE_CD": get_timesheet_type_code_by_period(),
                                    "TH___AUTO_ADJ_PCT_RT": 1,
                                    "TH___CORRECTING_REF_DT": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate']) if (get_timesheet_type_code_by_period() == 'C' or get_date_only(get_period_info()['TS_DT']) != get_date_only(get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate']))) else '',
                                    "TS_DT": get_period_info()['TS_DT'],
                                    "TS_HDR_SEQ_NO": get_timesheet_header_seq(rail.result('get_existing_deltek_timesheet')[0]) + (1 if is_revert_required(rail.result('get_existing_deltek_timesheet')[0]) else 0),
                                    "REFERENCE_SEQ_NO": get_r_type_row_data().get('REFERENCE_SEQ_NO', '') if is_revert_required(rail.result('get_existing_deltek_timesheet')[0]) else '',
                                    "REFERENCE_TS_TYPE_CD": get_r_type_row_data().get('REFERENCE_TS_TYPE_CD', '') if is_revert_required(rail.result('get_existing_deltek_timesheet')[0]) else '',
                                },
                                "children": get_children(rail.result('get_replicon_time_entries'), rail.result('get_replicon_task_details'),
                                                         rail.result('get_replicon_pay_codes'), rail.result(
                                                             'get_replicon_role_details'),
                                                         rail.result('get_replicon_account_details'), rail.result(
                                                             'get_replicon_project_details'),
                                                         rail.result('get_division_details'), rail.result('get_oef_tag_details'),
                                                         rail.result('get_replicon_user_details')[0]['userDetails']['uri'],
                                                         rail.result('get_replicon_timeoffs'),
                                                         rail.result('get_replicon_time_off_type_details'),
                                                         rail.result('get_replicon_task_allocations')
                                                         )
                            }
                        }
                    ]
                }
            }
        )

        is_timesheet_available = rail.IfOperator(
            task_id='is_timesheet_available',
            test=lambda: is_revert_required(
                rail.result('get_existing_deltek_timesheet')[0]),
            yes_task='get_reversing_record_data',
            no_task='get_replicon_role_details'
        )

        is_other_source_present = rail.IfOperator(
            task_id='is_other_source_present',
            test=lambda: is_other_source_project_present(
                rail.result('get_replicon_project_details')),
            yes_task='catch_error',
            no_task='get_division_details'
        )

        get_reversing_record_data = rail.PythonOperator(
            task_id='get_reversing_record_data',
            python_callable=lambda: get_reversing_record(
                rail.result('get_existing_deltek_timesheet')[0])
        )

        revert_existing_time = rail.DeltekCostPointServiceOperator(
            task_id='revert_existing_time',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda: rail.result('get_reversing_record_data')
        )

        get_replicon_pay_codes = rail.RepliconServiceOperator(
            task_id='get_replicon_pay_codes',
            endpoint='/services/PayCodeService1.svc/GetAllPayCodes',
        )

        get_account_details = rail.RepliconServiceOperator(
            task_id='get_replicon_account_details',
            endpoint='services/costcenterservice1.svc/BulkGetCostCenterDetails',
            data=lambda: {
                "costCenterUris": [rail.result('get_replicon_user_details')[0]['costCenterSchedule'][-1]['costCenter']['uri']] if rail.result('get_replicon_user_details')[0]['costCenterSchedule'] else []
            }
        )

        is_export_successful = rail.IfOperator(
            task_id='is_export_successful',
            test=lambda: rail.result('push_time_to_costpoint')[0][
                'MethodResponse']['Severity'] < 3,
            yes_task='catch_error',
            no_task='export_error'
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error:' +
            rail.render_template("{{get_error_message()}}")
        )

        get_project_entries = rail.PythonOperator(
            task_id='get_project_entries',
            python_callable=lambda: get_entry_projects(
                rail.result('get_replicon_time_entries'))
        )

        get_project_allocations = rail.RepliconServiceCallForEachItemOperator(
            app='polaris',
            task_id='get_project_allocations',
            items=lambda: rail.result('get_project_entries'),
            endpoint="graphql",
            method='POST',
            replicon_conn_id=config.replicon_conn_id,
            data={
                "variables":
                    {
                        "projectId": "{{ item }}",
                        "searchPhrase": "",
                        "allocationStatusList": ["COMMITTED"]},
                "query": "query Eager_projectResourcesQuery($projectSlug: String, $projectId: String, $searchPhrase: String, $allocationStatusList: [ResourceAllocationStatus!]) {\n  project(projectSlug: $projectSlug, projectId: $projectId) {\n    ...ProjectResources\n    __typename\n  }\n}\n\nfragment ProjectResources on Project {\n  id\n  resources(\n    searchPhrase: $searchPhrase\n    allocationStatusList: $allocationStatusList\n  ) {\n    totalItems\n    items {\n      id\n      uri\n      slug\n      displayText\n      isEnabled\n      projectRoles {\n        isPrimary\n        projectRole {\n          uri\n          name\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n"}
            )

        is_time_entry_against_project = rail.IfOperator(
            task_id='is_time_entry_against_project',
            test=lambda: is_time_entry_against_top_project(
                rail.result('get_replicon_time_entries')),
            yes_task='get_project_entries',
            no_task='get_replicon_pay_codes'
        )

        get_allocation_roles = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_allocation_roles',
            items=lambda: get_user_role_uris(
                rail.result('get_project_allocations'), rail.result('get_replicon_timesheet'), rail.result(
                    'get_replicon_user_details')[0]['userDetails']),
            endpoint="/services/ProjectRoleService1.svc/GetRoleDetails",
            data={
                "projectRoleUri": "{{ item }}",
                "asOfDate": null
            }
        )

        get_costpotint_work_force = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='get_costpotint_work_force',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            items=lambda: get_project_codes(
                rail.result('get_replicon_project_details')),
            data=lambda item: {
                "filter": {
                    "id": "polaris_exp_pjm_work",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJEMPL_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "=",
                                                "value": item
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        get_costpoint_project_plcs = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='get_costpoint_project_plcs',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            items=lambda: get_project_codes(
                rail.result('get_replicon_project_details')),
            data=lambda item: {
                "filter": {
                    "id": "polaris_exp_plc_prj",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJLABCAT_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PROJ_ID",
                                                "relation": "=",
                                                "value": item
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        verify_costpoint_configurations = rail.PythonOperator(
            task_id="verify_costpoint_configurations",
            python_callable=lambda: verify_costpoint_work_force()
        )

        verify_costpoint_plc_assignments = rail.PythonOperator(
            task_id="verify_costpoint_plc_assignments",
            python_callable=lambda: verify_costpoint_plcs()
        )

        is_costpoint_assignment_required = rail.IfOperator(
            task_id='is_costpoint_assignment_required',
            test=lambda: len(rail.result(
                'verify_costpoint_configurations')) > 0,
            yes_task='update_assignments_in_costpoint',
            no_task='push_time_to_costpoint'
        )

        update_assignments_in_costpoint = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='update_assignments_in_costpoint',
            items=lambda: get_unique_projects(),
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            data=lambda item: costpoint_workforce_modification_body(item)
        )

        is_missing_plc_assignment = rail.IfOperator(
            task_id='is_missing_plc_assignment',
            test=lambda: is_plc_assignment_required(
                rail.result('verify_costpoint_plc_assignments')),
            yes_task='assign_plc_to_project',
            no_task='get_costpotint_work_force'
        )

        assign_plc_to_project = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='assign_plc_to_project',
            endpoint='cpweb/cprestfulws/cpwwsgenericimport.cps',
            company=lambda: get_user_company(rail.result(
                'get_replicon_user_details')[0]['userDetails']),
            items=lambda: get_unique_projects_for_plc(
                rail.result('verify_costpoint_plc_assignments')),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True,
            data=lambda item: get_plc_assignments(
                item, rail.result('verify_costpoint_plc_assignments'))
        )

        def get_unique_projects_for_plc(missing_project_plcs):
            unique_projects = []
            for project_plc in missing_project_plcs:
                if project_plc["PROJ_ID"] and project_plc["PROJ_ID"] not in unique_projects:
                    unique_projects.append(project_plc["PROJ_ID"])
            return unique_projects

        def is_source_costpoint(user):
            if user and user["userDetails"] and user["userDetails"]["extensionFieldValues"]:
                extensionValue = rail.find_first_by_attr_and_get_attr(
                    user["userDetails"]["extensionFieldValues"], 'definition.displayText', config.user_source_oef_name, 'textValue')
                return True if (not extensionValue or extensionValue == config.user_source_oef_value) else False
            return True

        def get_timesheet_period_uri(user, timesheet):
            schedule = (user or {}).get('timesheetPeriodSchedule') or []
            for entry in schedule:
                if entry.get('effectiveDate') is None:
                    return entry['timesheetPeriod']['uri']
            end_date = get_formatted_date(timesheet['dateRange']['endDate'])
            applicable = [e for e in schedule if get_formatted_date(e['effectiveDate']) <= end_date]
            if not applicable:
                raise ValueError(f'No timesheet period schedule is effective as of {end_date} for this user')
            return max(applicable, key=lambda e: get_formatted_date(e['effectiveDate']))['timesheetPeriod']['uri']

        def get_timeoff_type_uris(time_offs):
            unique_timeoff_types = []
            if time_offs:
                for time_off in time_offs:
                    time_off_type = time_off.get('timeOffType')
                    if time_off_type :
                        time_off_type_uri = time_off_type.get('uri')
                        if time_off_type_uri not in unique_timeoff_types:
                            unique_timeoff_types.append(time_off_type_uri)
            return unique_timeoff_types
        
        def get_plc_assignments(project_code, plc_modifications):
            project_plcs = []
            for plc in plc_modifications:
                if plc["PROJ_ID"] == project_code:
                    project_plcs.append({
                        "code": plc["BILL_LAB_CAT_CD"]
                    })
            return {
                "document": {
                    "id": "polaris_imp_plc_pj",
                    "rows": [
                        {
                            "row": {
                                "rsId": "PJM_PROJLABCAT_HDR",
                                "tranType": "INSERT",
                                "data": {
                                    "PROJ_ID": project_code
                                },
                                "children": get_plc_children(project_plcs)
                            }
                        }
                    ]
                }
            }

        def get_plc_children(plc_modifications):
            children = []
            if plc_modifications:
                for plc in plc_modifications:
                    children.append({
                        "row": {
                            "rsId": "PJM_PROJLABCAT_CTW",
                            "tranType": "INSERT",
                            "data": {
                                "BILL_LAB_CAT_CD": plc["code"]
                                # ,
                                # "BILL_LAB_CAT_DESC": plc["name"]
                            }
                        }
                    })
            return children

        def is_plc_assignment_required(plc_modifications):
            if plc_modifications and len(plc_modifications) > 0:
                return True
            return False

        def costpoint_workforce_modification_body(project_Id):
            modifications = rail.result('verify_costpoint_configurations')
            return {
                "document": {
                    "id": "polaris_imp_pjmwork",
                    "rows": get_modification_rows(modifications, project_Id)
                }
            }

        def get_unique_projects():
            modifications = rail.result('verify_costpoint_configurations')
            projects = []
            for modification in modifications:
                if modification["PROJ_ID"] not in projects:
                    projects.append(modification["PROJ_ID"])
            return projects

        def get_modification_rows(resourceModification, project_id):
            tranType = 'UPDATE' if resourceModification[0]["DFLT_FLG"] else 'INSERT'
            return [
                {
                    "row": {
                        "rsId": "PJM_PROJEMPL_HDR",
                        "tranType": tranType,
                        "data": {
                            "OT_AUTH_FL": "N",
                            "PROJ_ID": project_id
                        },
                        "children": get_resource_children(resourceModification, project_id)
                    }
                }
            ]

        def is_other_source_project_present(projects):
            is_other_project_source = False
            for project in projects:
                is_other_project_source = is_other_project_source or not is_cost_point_project_source(project)
            return is_other_project_source

        def is_cost_point_project_source(project):
            if project and project['projectDetails']['customFields']:
                customFieldValue = rail.find_first_by_attr_and_get_attr(
                    project['projectDetails']['customFields'], 'customField.displayText', config.project_source_custom_field_name, 'text')
            if customFieldValue:
                return customFieldValue == config.project_source_custom_field_value
            return True
        
        def get_resource_children(resourceModifications, project):
            children = []
            added_users = []
            added_plcs = []
            emp_plcs = []
            if resourceModifications:
                for resource in resourceModifications:
                    if resource["PROJ_ID"] == project:
                        if resource["EMPL_ID"].upper() not in added_users and resource["DFLT_FLG"] == "Y":
                            added_users.append(resource["EMPL_ID"].upper())
                            children.append({
                                "row": {
                                    "rsId": "PJM_PROJEMPL_CHILDTO",
                                    "tranType": "INSERT",
                                    "data": {
                                        "EMPL_ID": resource["EMPL_ID"].upper()
                                    }
                                }
                            })
                        if (resource["BILL_LAB_CAT_CD"] and resource["BILL_LAB_CAT_CD"] not in added_plcs):
                            added_plcs.append(resource["BILL_LAB_CAT_CD"])
                            emp_plcs.append({
                                "row": {
                                    "rsId": "PJM_PROJEMPLLABCAT_PLCWK",
                                    "tranType": "INSERT",
                                    "data": {
                                        "BILL_LAB_CAT_CD": resource["BILL_LAB_CAT_CD"],
                                        "DFLT_FL": resource["DFLT_FLG"],
                                        "PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID": resource["EMPL_ID"].upper()
                                    }
                                }
                            })

            children.append({"row": {
                "rsId": "PJM_PROJEMPL_LABCAT_PLCWKFRCE",
                "tranType": "SELECT",
                "data": {
                },
                "children": emp_plcs}})

            return children

        def verify_costpoint_work_force():
            costPoint_workforce = rail.result('get_costpotint_work_force')
            time_entries_to_sync = rail.result('get_time_entries_to_sync')
            missing_assignments = get_missing_assignments(
                costPoint_workforce, time_entries_to_sync)
            return missing_assignments

        def verify_costpoint_plcs():
            costPoint_project_plcs = rail.result('get_costpoint_project_plcs')
            time_entries_to_sync = rail.result('get_time_entries_to_sync')
            missing_project_plcs = get_missing_plcs(
                costPoint_project_plcs, time_entries_to_sync)
            return missing_project_plcs

        def get_missing_plcs(costPointWorkForce, polarisTimeEntries):
            missingPlcs = []
            added_project_plcs = []
            if polarisTimeEntries and polarisTimeEntries["document"] and polarisTimeEntries["document"]["rows"] and \
                    polarisTimeEntries["document"]["rows"][0] and polarisTimeEntries["document"]["rows"][0]["row"] \
                    and polarisTimeEntries["document"]["rows"][0]["row"]["children"] and \
                    len(polarisTimeEntries["document"]["rows"][0]["row"]["children"]) > 0:
                for child in polarisTimeEntries["document"]["rows"][0]["row"]["children"]:
                    if child and child["row"] and child["row"]["data"]:
                        parent_project_id = child["row"]["data"]["PROJ_ID"].split(".")[
                            0]
                        plc = child["row"]["data"]["BILL_LAB_CAT_CD"]
                        if plc is None or not plc:
                            plc = get_user_role_code()
                        if plc:
                            plc = plc.upper()
                        if plc:
                            #assign plc only when plc is present
                            if parent_project_id + "__" + plc not in added_project_plcs:
                                added_project_plcs.append(
                                    parent_project_id + "__" + plc)
                                project_cp_plcs = filter_proj_work_force(
                                    costPointWorkForce, parent_project_id)
                                isPlcPreset = is_plc_present_in_costPoint(
                                    plc, project_cp_plcs)
                                if not isPlcPreset:
                                    missingPlcs.append({
                                        "PROJ_ID": parent_project_id,
                                        "BILL_LAB_CAT_CD": plc
                                    })
            return missingPlcs

        def get_missing_assignments(costPointWorkForce, polarisTimeEntries):
            missingAssignments = []
            added_projects = []
            added_project_plcs = []
            if polarisTimeEntries and polarisTimeEntries["document"] and polarisTimeEntries["document"]["rows"] and \
                    polarisTimeEntries["document"]["rows"][0] and polarisTimeEntries["document"]["rows"][0]["row"] \
                    and polarisTimeEntries["document"]["rows"][0]["row"]["children"] and \
                    len(polarisTimeEntries["document"]["rows"][0]["row"]["children"]) > 0:
                for child in polarisTimeEntries["document"]["rows"][0]["row"]["children"]:
                    if child and child["row"] and child["row"]["data"]:
                        parent_project_id = child["row"]["data"]["PROJ_ID"].split(".")[
                            0]
                        plc = child["row"]["data"].get("BILL_LAB_CAT_CD") or \
                            child["row"]["data"].get("PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD")
                        if plc is None or not plc:
                            plc = get_user_role_code()
                        if plc:
                            plc = plc.upper()
                        if plc:
                            if parent_project_id + "__" + plc not in added_project_plcs:
                                added_project_plcs.append(
                                    parent_project_id + "__" + plc)
                                project_cp_workforce = filter_proj_work_force(
                                    costPointWorkForce, parent_project_id)
                                assignment_details = is_assignment_present_in_costPoint(
                                    plc, project_cp_workforce, polarisTimeEntries["document"]["rows"][0]["row"]["data"]["EMPL_ID"])
                                dflt_flg = "Y"

                                if assignment_details and not assignment_details["is_present"]:
                                    if assignment_details and assignment_details["is_employee_present"]:
                                        dflt_flg = "N"
                                    if parent_project_id in added_projects:
                                        dflt_flg = "N"

                                    missingAssignments.append({
                                        "EMPL_ID": polarisTimeEntries["document"]["rows"][0]["row"]["data"]["EMPL_ID"],
                                        "PROJ_ID": parent_project_id,
                                        "BILL_LAB_CAT_CD": plc,
                                        "DFLT_FLG": dflt_flg
                                    })
                                    added_projects.append(parent_project_id)
            return missingAssignments

        def get_user_role_code():
            assignedRoles = rail.result('get_user_role')
            role_uri = get_user_role_uri(assignedRoles)
            role_details = rail.result('get_replicon_role_details')
            role = get_role_discription(role_uri, role_details)
            return role

        def filter_proj_work_force(costPointWorkForce, project_code):
            if costPointWorkForce and project_code:
                for project_workForce in costPointWorkForce:
                    if project_workForce["row"] and project_workForce["row"]["data"] and \
                            project_workForce["row"]["data"]["PROJ_ID"] == project_code:
                        return project_workForce

        def is_plc_present_in_costPoint(plc, projectPlcs):
            if projectPlcs and projectPlcs["row"] and projectPlcs["row"]["children"]:
                for child in projectPlcs["row"]["children"]:
                    if child["row"] and child["row"]["data"] and \
                            (("BILL_LAB_CAT_CD" in child["row"]["data"] and child["row"]["data"]["BILL_LAB_CAT_CD"] == plc) or
                                ("PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD" in child["row"]["data"] and child["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD"] == plc)):
                        return True
            return False

        def is_assignment_present_in_costPoint(plc, costPointWorkForce, employeeId):
            is_empl_assignement_present = False
            is_assignment_present = False
            if costPointWorkForce and costPointWorkForce['row'] and \
                    costPointWorkForce['row']['children'] and len(costPointWorkForce['row']['children']):
                for child in costPointWorkForce['row']['children']:
                    if child["row"]["rsId"] == "PJM_PROJEMPL_LABCAT_PLCWKFRCE":
                        if child["row"]["children"]:
                            for plcChild in child["row"]["children"]:
                                if plcChild["row"]["rsId"] == "PJM_PROJEMPLLABCAT_PLCWK" and \
                                        plcChild["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_EMPL_ID"].upper() == employeeId.upper():
                                    is_empl_assignement_present = True
                                    if plcChild["row"]["data"]["PJM_PROJEMPLLABCAT_PLCWK_BILL_LAB_CAT_CD"].upper() == plc or \
                                            plcChild["row"]["data"].get("BILL_LAB_CAT_CD", "").upper() == plc:
                                        is_assignment_present = True
                                        break

            return {
                "is_present": is_assignment_present,
                "is_employee_present": is_empl_assignement_present
            }

        def get_project_codes(projects):
            project_codes = []
            if projects:
                for project in projects:
                    if project and project["projectDetails"] and project["projectDetails"]["code"]:
                        code = project["projectDetails"]["code"].split(".")[0]
                        if code not in project_codes:
                            project_codes.append(code)
            return project_codes

        def get_user_role_uris(projectAllocations, timesheetDetails, userDetails):
            role_uris = []
            user_uri = timesheetDetails['owner']['uri']
            for allocation in projectAllocations:
                if allocation["data"] and allocation["data"]["project"] and allocation["data"]["project"]["resources"] \
                        and allocation["data"]["project"]["resources"]["items"]:
                    for item in allocation["data"]["project"]["resources"]["items"]:
                        if item["uri"] == user_uri and item["projectRoles"]:
                            for role in item["projectRoles"]:
                                if (role["projectRole"]):
                                    role_uris.append(
                                        role["projectRole"]["uri"])
            return role_uris

        def get_user_company(userDetails):
            company = rail.find_first_by_attr_and_get_attr(
                userDetails['extensionFieldValues'], 'definition.displayText', 'Company', 'textValue')
            return [company]

        def is_time_entry_against_top_project(entries):
            for entry in entries:
                if is_project_time_entry(entry):
                    return True
            return False

        def is_project_time_entry(entry):
            project_uri = rail.find_first_by_attr_and_get_attr(
                entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:project', 'value.uri')
            task_uri = get_task_uri(entry)
            if project_uri and (task_uri is None):
                return True
            return False

        def get_entry_projects(entries):
            project_uris = []
            for entry in entries:
                project_uri = rail.find_first_by_attr_and_get_attr(
                    entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:project', 'value.uri')
                if project_uri and is_project_time_entry(entry) and project_uri not in project_uris:
                    project_uris.append(project_uri)
            return project_uris

        def is_revert_required(existingTimesheet):
            if len(existingTimesheet['document']['rows']) > 0:
                reversingRecord = get_reversing_record(existingTimesheet)
                if reversingRecord is not None:
                    return True
            return False

        def get_export_message():
            timesheet_info = rail.result('get_replicon_timesheet')
            costpoint_response = rail.result('push_time_to_costpoint')
            if timesheet_info:
                if costpoint_response:
                    return rail.render_template('''Time sync failed for user "{{ result('get_replicon_timesheet').owner.loginName }}" \
                        and timesheet startdate "{{ result('get_replicon_timesheet').dateRange.startDate.month }}/{{ result('get_replicon_timesheet').dateRange.startDate.day }}\
                            /{{ result('get_replicon_timesheet').dateRange.startDate.year }}" message from api "{{ result('push_time_to_costpoint') }}"''')
                return rail.render_template('''Time sync failed for "{{ result('get_replicon_timesheet').owner.loginName }}" \
                    timesheet startdate "{{ result('get_replicon_timesheet').dateRange.startDate.month }}/{{ result('get_replicon_timesheet').dateRange.startDate.day }}\
                            /{{ result('get_replicon_timesheet').dateRange.startDate.year }}"''')
            return rail.render_template('''Time sync failed for the timesheet "{{ dag_run.conf.webhook.data.timesheet.uri }}"''')

        export_error = rail.PythonOperator(
            task_id="export_error",
            python_callable=get_export_message
        )

        send_error = rail.EmailOperator(
            task_id='send_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Timesheet Sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong>
            <br /> <br />Hello, <br /> <br /> {{ result('export_error') }}
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        def get_reversing_record(costpointTimesheet):
            if costpointTimesheet:
                processedRows = []
                for row in costpointTimesheet['document']['rows']:
                    if (row['row']['data'].get('TH___CORRECTING_REF_DT') == get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate']) or row['row']['data'].get('TH___CORRECTING_REF_DT') is None) and row['row']['data']['S_TS_TYPE_CD'] in ('R', 'C') and row['row']['data']['REG_HRS'] > 0 \
                            and not is_data_reversed(row, costpointTimesheet['document']['rows'], processedRows):
                        return change_to_reversed(row)
            return None

        def change_to_reversed(row):
            period = get_period_info() if getattr(config, 'sync_to_open_period', False) else row['row']['data']
            return {
                "document": {
                    "id": "polaris_imp_ldmtime",
                    "rows": [
                        {
                            "row": {
                                "rsId": "LDMTIME_TSHDR",
                                "tranType": "INSERT",
                                "data": {
                                    "EMPL_ID": row['row']['data']['EMPL_ID'],
                                    "FY_CD": period['FY_CD'],
                                    "OTH_HRS": -1.0 * row['row']['data']["OTH_HRS"],
                                    "PD_NO": period['PD_NO'],
                                    "REG_HRS": -1.0 * row['row']['data']["REG_HRS"],
                                    "SUB_PD_NO": period['SUB_PD_NO'],
                                    "S_TS_TYPE_CD": 'C',
                                    "REFERENCE_SEQ_NO": row['row']['data']['TS_HDR_SEQ_NO'],
                                    "REFERENCE_TS_TYPE_CD": "R",
                                    "TH___CORRECTING_REF_DT": get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate']),
                                    "TH___AUTO_ADJ_PCT_RT": row['row']['data']['TH___AUTO_ADJ_PCT_RT'],
                                    "TS_DT": period['TS_DT'],
                                    "TS_HDR_SEQ_NO": get_timesheet_header_seq(rail.result('get_existing_deltek_timesheet')[0])
                                },
                                "children": get_revert_children(row['row']['children'])
                            }
                        }
                    ]
                }
            }
        
        def get_timesheet_type_code():
            if(is_revert_required(rail.result('get_existing_deltek_timesheet')[0])):
                return getattr(config, 'reversal_timesheet_type', 'R')
            return 'R'

        def get_timesheet_type_code_by_period():
            timesheet_end_date = get_date_only(get_formatted_date(rail.result('get_replicon_timesheet')['dateRange']['endDate']))
            current_period = rail.result('get_current_timesheet_period')
            if current_period and get_date_only(current_period.get('END_DT')) == timesheet_end_date:
                existing_ts = rail.result('get_existing_deltek_timesheet')[0]
                if existing_ts and existing_ts.get('document') and existing_ts['document'].get('rows'):
                    for row in existing_ts['document']['rows']:
                        data = row['row']['data']
                        if ((get_date_only(data.get('TS_DT')) == timesheet_end_date and
                              (not data.get('TH___CORRECTING_REF_DT') or get_date_only(data.get('TH___CORRECTING_REF_DT')) == timesheet_end_date))
                              or get_date_only(data.get('TH___CORRECTING_REF_DT')) == timesheet_end_date):
                            return 'C'
                return 'R'
            else:
                existing_ts = rail.result('get_existing_deltek_timesheet')[0]
                if existing_ts and existing_ts.get('document') and existing_ts['document'].get('rows'):
                    for row in existing_ts['document']['rows']:
                        data = row['row']['data']
                        if ((get_date_only(data.get('TS_DT')) == timesheet_end_date and
                                not data.get('TH___CORRECTING_REF_DT')) or get_date_only(data.get('TH___CORRECTING_REF_DT')) == timesheet_end_date):
                            return 'C'
            return 'R'

        def get_revert_children(children):
            revertChildren = []
            for child in children:
                if child['row']['rsId'] == 'LDMTIME_TSLN':
                    revertChildren.append(
                        {"row": {
                            "rsId": "LDMTIME_TSLN",
                            "tranType": "INSERT",
                            "data": {
                                "ACCT_ID": child['row']['data']['ACCT_ID'],
                                "BILL_LAB_CAT_CD": child['row']['data']['BILL_LAB_CAT_CD'],
                                "GENL_LAB_CAT_CD": child['row']['data']['GENL_LAB_CAT_CD'],
                                "ORG_ID": child['row']['data']['ORG_ID'],
                                "PAY_TYPE": child['row']['data']['PAY_TYPE'],
                                "PROJ_ID": child['row']['data']['PROJ_ID'],
                                "TS_LN___CHG_HRS": -1.0 * child['row']['data']['TS_LN___CHG_HRS'],
                                "TS_LN___LAB_LOC_CD": child['row']['data']['TS_LN___LAB_LOC_CD'],
                                "TS_LN___S_TS_LN_TYPE_CD": child['row']['data']['TS_LN___S_TS_LN_TYPE_CD'],
                                "TS_LN___WORK_COMP_CD": child['row']['data']['TS_LN___WORK_COMP_CD']
                            }
                        }
                        }
                    )
            return revertChildren

        def is_data_reversed(row, rows, processedRows):
            processedRows.append(row)
            for item in rows:
                if not is_already_processed(processedRows, item):
                    if item['row']['data']['REG_HRS'] == -1.0  *row['row']['data']['REG_HRS'] and \
                        item['row']['data']["OTH_HRS"] == -1.0 * row['row']['data']["OTH_HRS"]:
                        processedRows.append(item)
                        return True
            return False

        def is_already_processed(processedRows, item):
            for row in processedRows:
                if row['row']['data']['FY_CD'] == item['row']['data']['FY_CD'] and \
                        row['row']['data']['PD_NO'] == item['row']['data']['PD_NO'] and \
                        row['row']['data']['SUB_PD_NO'] == item['row']['data']['SUB_PD_NO']:
                    return True
            return False

        def get_r_type_row_data():
            reversing_record = rail.result('get_reversing_record_data')
            if not reversing_record or not reversing_record.get('document'):
                return {}
            rows = reversing_record['document'].get('rows') or []
            return rows[0]['row']['data'] if rows else {}

        def get_project_uris(tasks):
            projectUris = []
            projects = []
            project_entries = rail.result('get_project_entries')
            if tasks:
                for task in tasks:
                    projectUri = task['project']['uri']
                    if projectUri not in projectUris:
                        projects.append({"uri": projectUri})
                        projectUris.append(projectUri)
            if project_entries:
                for project in project_entries:
                    if project not in projectUris:
                        projects.append({"uri": project})
                        projectUris.append(project)
            return projects

        def get_division_uris(projects):
            divisions = []
            if projects:
                for project in projects:
                    division = project['projectDetails']['division']
                    if division and division['uri'] and division['uri'] not in divisions:
                        divisions.append(division['uri'])
            return divisions

        def get_timesheet_header_seq(existingTimesheet):
            open_period_timesheet = rail.result('get_existing_deltek_timesheet_open_period')
            if getattr(config, 'sync_to_open_period', False) and open_period_timesheet:
                existingTimesheet = open_period_timesheet[0]
            if not existingTimesheet or \
                not existingTimesheet['document'] \
                    or len(existingTimesheet['document']['rows']) == 0:
                return 1
            seq_no = get_max_header_seq(existingTimesheet) + 1
            return seq_no

        def get_max_header_seq(existingTimesheet):
            seq_no = 1
            for row in existingTimesheet['document']['rows']:
                if row['row']['data']['TS_HDR_SEQ_NO'] > seq_no:
                    seq_no = row['row']['data']['TS_HDR_SEQ_NO']
            return seq_no

        def get_period_number(timesheet):
            return timesheet['dateRange']['endDate']['month']

        def get_financial_year(timesheet):
            return timesheet['dateRange']['endDate']['year']

        def get_period_info():
            if getattr(config, 'sync_to_open_period', False):
                subperiod = get_open_subperiod(rail.result('get_open_subperiods'), rail.result('get_replicon_timesheet'))
                return {'FY_CD': subperiod['FY_CD'], 'PD_NO': subperiod['PD_NO'], 'SUB_PD_NO': subperiod['SUB_PD_NO'], 'TS_DT': rail.result('get_current_timesheet_period')['END_DT']}
            timesheet = rail.result('get_replicon_timesheet')
            return {'FY_CD': get_financial_year(timesheet), 'PD_NO': get_period_number(timesheet), 'SUB_PD_NO': 1, 'TS_DT': get_timesheet_date(timesheet)}

        def get_open_subperiod(subperiods, timesheet):
            if isinstance(subperiods, list):
                subperiods = subperiods[0] if subperiods else None
            timesheet_end_date = get_formatted_date(timesheet['dateRange']['endDate'])
            if subperiods and subperiods.get('document') and subperiods['document'].get('rows'):
                matching = None
                for row in subperiods['document']['rows']:
                    sub_pd_end_dt = row['row']['data']['SUB_PD_DISP_END_DT']
                    if sub_pd_end_dt >= timesheet_end_date:
                        is_open = any(
                            child['row']['data'].get('OPEN_FL') == 'O'
                            for child in (row['row'].get('children') or [])
                            if (child.get('row') or {}).get('rsId') == 'GLMSUBPD_SUBPDJNLSTATUS_CTW'
                        )
                        if is_open and (matching is None or sub_pd_end_dt < matching['row']['data']['SUB_PD_DISP_END_DT']):
                            matching = row
                if matching:
                    return matching['row']['data']
            raise ValueError(f'No open subperiod exists in Costpoint for timesheet end date {timesheet_end_date}')

        def get_date_only(date_str):
            return date_str[:10] if date_str else date_str

        def get_current_costpoint_timesheet_period(costpoint_periods, timesheet):
            if isinstance(costpoint_periods, list):
                costpoint_periods = costpoint_periods[0] if costpoint_periods else None
            timesheet_end_date = get_date_only(get_formatted_date(timesheet['dateRange']['endDate']))
            if costpoint_periods and costpoint_periods.get('document') and costpoint_periods['document'].get('rows'):
                matching = None
                for header in costpoint_periods['document']['rows']:
                    header_row = header.get('row') or {}
                    for child in (header_row.get('children') or []):
                        child_row = child.get('row') or {}
                        if child_row.get('rsId') == 'LDMTSPD_TSPDSCH_DTL':
                            data = child_row.get('data') or {}
                            end_dt = get_date_only(data.get('END_DT'))
                            if end_dt and end_dt >= timesheet_end_date and data.get('OPEN_FL') == 'Y':
                                if matching is None or end_dt < get_date_only(matching['END_DT']):
                                    matching = data
                if matching:
                    return matching
            raise ValueError(f'No open timesheet period exists in Costpoint for timesheet end date {timesheet_end_date}')

        def get_task_uris(entries):
            taskUris = []
            for entry in entries:
                task_uri = get_task_uri(entry)
                if task_uri and task_uri not in taskUris:
                    taskUris.append(task_uri)
            return taskUris

        def get_oef_tag_uris(entries):
            tagUris = []
            payTypeOefName = Variable.get(
                config.pay_type_oef_var_name, default_var='Pay Type')
            if payTypeOefName:
                for entry in entries:
                    tagUri = rail.find_first_by_attr_and_get_attr(
                        entry['extensionFieldValues'], 'definition.displayText', payTypeOefName, 'tag.uri')
                    if tagUri and tagUri not in tagUris:
                        tagUris.append(tagUri)
            return tagUris

        def get_role_uris(tasks, timeEntries):
            roleUris = []
            for task in tasks:
                roleUri = rail.find_first_by_attr_and_get_attr(
                    task['keyValues'], 'keyUri', 'urn:replicon:task-key-value-key:assigned-role', 'value.uri')
                if roleUri and roleUri not in roleUris:
                    roleUris.append(roleUri)
            user_role = get_user_role_uri(rail.result('get_user_role'))
            if user_role and user_role not in roleUris:
                roleUris.append(user_role)
            
            userUri = rail.result('get_replicon_user_details')[0]['userDetails']['uri']
            taskAllocations = rail.result('get_replicon_task_allocations')
            if taskAllocations: 
                for entry in timeEntries:
                    if is_project_allocation_type(entry):
                        roleUri = get_allocation_role_uri(entry, tasks, userUri, taskAllocations)
                        if roleUri and roleUri not in roleUris:
                            roleUris.append(roleUri)
            return roleUris

        def get_user_role_uri(assignedRoles):
            role_Uri = None
            if assignedRoles and assignedRoles[-1] and assignedRoles[-1]["projectRoles"] \
                    and assignedRoles[-1]["projectRoles"][0]["projectRole"]:
                role_Uri = assignedRoles[-1]["projectRoles"][0]["projectRole"]["uri"]
            return role_Uri

        def get_timesheet_date(timesheet):
            return get_formatted_date(timesheet['dateRange']['endDate'])

        def get_children(entries, taskDetails, payCodes, roles, accountDetails, projects, 
                         divisions, oefTags, userUri, timeOffs, timeoffTypeDetails, taskAllocations):
            childRows = []
            projectEntries = {}
            send_blank_plc_account_org = getattr(config, 'send_blank_plc_account_org', False)
            if Variable.get(config.group_by_project_var_name, default_var='false') == '1':
                for entry in entries:
                    if is_project_allocation_type(entry):
                        projectId = get_project_id(
                            entry, taskDetails, projects)
                        plc = get_billing_labor_category(
                            entry, taskDetails, roles, userUri, taskAllocations, send_blank_plc_account_org)
                        payType = get_pay_type(entry, payCodes, oefTags)
                        key = projectId + "_" + plc + "_" + payType

                        if key in projectEntries:
                            projectEntries[key].append(entry)
                        else:
                            projectEntries[key] = [entry]
                for key, projectEntry in projectEntries.items():
                    childRows.append({
                        "row": {
                            "rsId": "LDMTIME_TSLN",
                            "tranType": "INSERT",
                            "data": {
                                "ACCT_ID": get_account_id(accountDetails, send_blank_plc_account_org),
                                "BILL_LAB_CAT_CD": get_billing_labor_category(projectEntry[0], taskDetails, roles, userUri, taskAllocations, send_blank_plc_account_org),
                                "ORG_ID": get_org_id(projectEntry[0], taskDetails, projects, divisions, send_blank_plc_account_org),
                                "PAY_TYPE": get_pay_type(projectEntry[0], payCodes, oefTags),
                                "PROJ_ID": get_project_id(projectEntry[0], taskDetails, projects),
                                "TS_LN___CHG_HRS": get_total_hours(projectEntry, payCodes),
                                "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code(projectEntry[0]),
                            }
                        }
                    })
            else:
                for entry in entries:
                    if is_project_allocation_type(entry):
                        if get_total_hours([entry], payCodes) != 0:
                            childRows.append(
                                {
                                    "row": {
                                        "rsId": "LDMTIME_TSLN",
                                        "tranType": "INSERT",
                                        "data": {
                                            "ACCT_ID": get_account_id(accountDetails, send_blank_plc_account_org),
                                            "BILL_LAB_CAT_CD": get_billing_labor_category(entry, taskDetails, roles, userUri, taskAllocations, send_blank_plc_account_org),
                                            "ORG_ID": get_org_id(entry, taskDetails, projects, divisions, send_blank_plc_account_org),
                                            "PAY_TYPE": get_pay_type(entry, payCodes, oefTags),
                                            "PROJ_ID": get_project_id(entry, taskDetails, projects),
                                            "TS_LN_DT": get_line_date(entry),
                                            "TS_LN___CHG_HRS": get_total_hours([entry], payCodes),
                                            "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code(entry),
                                            "TS_LN___NOTES": get_comments(entry)
                                        }
                                    }
                                }
                            )
            if timeOffs:
                for time_off in timeOffs:
                    for entry in time_off['entries']:
                        time_off_type_uri = time_off['timeOffType']['uri']
                        childRows.append(
                                {
                                    "row": {
                                        "rsId": "LDMTIME_TSLN",
                                        "tranType": "INSERT",
                                        "data": {
                                            "ACCT_ID": "",
                                            "BILL_LAB_CAT_CD": "",
                                            "ORG_ID": "",
                                            "PAY_TYPE": get_time_off_pay_type(time_off_type_uri, timeoffTypeDetails, payCodes),
                                            "PROJ_ID": get_time_off_project_id(time_off_type_uri, timeoffTypeDetails),
                                            "TS_LN_DT": get_line_date(entry),
                                            "TS_LN___CHG_HRS": get_total_hours([], payCodes, [{"entries":[entry]}]),
                                            "TS_LN___S_TS_LN_TYPE_CD": get_line_type_code(entry),
                                            "TS_LN___NOTES": time_off['comments'][0:254] if time_off['comments'] else ''
                                        }
                                    }
                                }
                            ) 
            return childRows
        
        def get_time_off_project_id(time_off_type_uri, time_off_type_details):
            time_off_description = rail.find_first_by_attr_and_get_attr(
                    time_off_type_details, 'uri', time_off_type_uri, 'description')
            return time_off_description

        def get_time_off_pay_type(time_off_type_uri, time_off_type_details, pay_codes):
            paycode_uri = rail.find_first_by_attr_and_get_attr(
                    time_off_type_details, 'uri', time_off_type_uri, 'payCode.uri')
            paycode_code = rail.find_first_by_attr_and_get_attr(pay_codes, 'uri', paycode_uri, 'code')
            return paycode_code
        
        def get_comments(entry):
            if entry and entry['customMetadata']:
                comment = rail.find_first_by_attr_and_get_attr(
                    entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:comments', 'value.text')
                return comment[0:254] if comment else ''
            return ''

        def is_project_allocation_type(entry):
            if entry and entry['timeAllocationTypeUris'] \
                    and 'urn:replicon:time-allocation-type:project' in entry['timeAllocationTypeUris']:
                return True
            return False

        def get_line_type_code(entry):
            return config.line_type if entry else None

        def get_line_date(entry):
            return get_formatted_date(entry['entryDate'])

        def get_formatted_date(dateObject):
            return f"{dateObject['year']}-{str(dateObject['month']).zfill(2)}-{str(dateObject['day']).zfill(2)}T00:00:00"

        def get_project_id(entry, taskDetails, projects):
            taskuri = get_task_uri(entry)
            if taskuri:
                for task in taskDetails:
                    if task['uri'] == taskuri:
                        task_code = task['code']
                        task_codes = task_code.split('-')
                        return task_codes[1].strip() if len(task_codes) == 2 else task_codes[0].strip()
            else:
                if is_project_time_entry(entry):
                    project_uri = get_entry_project_uri(entry)
                    if project_uri:
                        return rail.find_first_by_attr_and_get_attr(
                            projects, 'projectDetails.uri', project_uri, 'projectDetails.code')
            return ""

        def get_entry_project_uri(entry):
            return rail.find_first_by_attr_and_get_attr(
                entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:project', 'value.uri')

        def filter_project_allocation(project_allocations, project_uri, user_uri):
            for allocation in project_allocations:
                if allocation["data"] and allocation["data"]["project"] and allocation["data"]["project"]["id"] \
                        and allocation["data"]["project"]["id"] == project_uri:
                    for item in allocation["data"]["project"]["resources"]["items"]:
                        if item["uri"] == user_uri:
                            return item["projectRoles"][0]["projectRole"]
            return None

        def get_task_uri(entry):
            return rail.find_first_by_attr_and_get_attr(
                entry['customMetadata'], 'keyUri', 'urn:replicon:time-entry-metadata-key:task', 'value.uri')

        def get_role_discription(roleUri, roles):
            role = rail.find_first_by_attr_and_get_attr(
                roles, 'uri', roleUri, 'description')
            return role

        def get_allocation_role_uri(entry, tasks, userUri, taskAllocations):
            roleUri = ''            
            if not config.do_not_pass_polaris_plc and entry:
                taskUri = get_task_uri(entry)
                
                if taskUri:
                    if config.use_task_based_allocation:
                        roleUri = filter_task_allocations(taskAllocations, taskUri, userUri)
                    else:
                        keyValues = rail.find_first_by_attr_and_get_attr(
                            tasks, 'uri', taskUri, 'keyValues')
                        roleUri = rail.find_first_by_attr_and_get_attr(
                            keyValues, 'keyUri', 'urn:replicon:task-key-value-key:assigned-role', 'value.uri')
                else:
                    if is_project_time_entry(entry):
                        project_uri = get_entry_project_uri(entry)
                        project_allocations = rail.result(
                            'get_project_allocations')
                        if project_allocations:
                            project_allocation = filter_project_allocation(
                                project_allocations, project_uri, userUri)
                            if project_allocation:
                                roleUri = project_allocation["uri"]
            return roleUri

        def get_billing_labor_category(entry, tasks, roles, userUri, taskAllocations, send_blank_plc_account_org):
            if send_blank_plc_account_org:
                return ''
            role = ''
            if not config.do_not_pass_polaris_plc and entry:
                taskUri = get_task_uri(entry)
                
                if taskUri:
                    if config.use_task_based_allocation:
                        roleUri = filter_task_allocations(taskAllocations, taskUri, userUri)
                    else:
                        keyValues = rail.find_first_by_attr_and_get_attr(
                            tasks, 'uri', taskUri, 'keyValues')
                        roleUri = rail.find_first_by_attr_and_get_attr(
                            keyValues, 'keyUri', 'urn:replicon:task-key-value-key:assigned-role', 'value.uri')
                    if roleUri:
                        role = get_role_discription(roleUri, roles)
                else:
                    if is_project_time_entry(entry):
                        project_uri = get_entry_project_uri(entry)
                        project_allocations = rail.result(
                            'get_project_allocations')
                        project_roles = rail.result('get_allocation_roles')
                        if project_allocations:
                            project_allocation = filter_project_allocation(
                                project_allocations, project_uri, userUri)
                            if project_allocation:
                                roleUri = project_allocation["uri"]
                                if roleUri:
                                    role = get_role_discription(
                                        roleUri, project_roles)
            return role

        def filter_task_allocations(taskAllocations, taskUri, userUri):
            for taskAllocation in taskAllocations:
                for allocation in taskAllocation:
                    if allocation["task"] and allocation["task"]["uri"] == taskUri and \
                        allocation["resource"] and allocation["resource"]["uri"] == userUri:
                        return allocation["projectRole"]["uri"] if allocation["projectRole"] else None
            return None
        
        def get_org_id(entry, tasks, projects, divisions, send_blank_plc_account_org):
            if send_blank_plc_account_org:
                return ''
            if entry:
                taskUri = get_task_uri(entry)
                if taskUri:
                    projectUri = rail.find_first_by_attr_and_get_attr(
                        tasks, 'uri', taskUri, 'project.uri')
                    if projectUri:
                        divisionUri = rail.find_first_by_attr_and_get_attr(
                            projects, 'projectDetails.uri', projectUri, 'projectDetails.division.uri')
                        if divisionUri:
                            return rail.find_first_by_attr_and_get_attr(
                                divisions, 'uri', divisionUri, 'code')
            return ''

        def get_account_id(costCenterDetails, send_blank_plc_account_org):
            if send_blank_plc_account_org:
                return ''
            if costCenterDetails and costCenterDetails[0]:
                return costCenterDetails[0].get('code', '')
            return ''

        def get_pay_type(entry, allPaycodes, oefTags):
            paycode = get_pay_code(entry, allPaycodes, oefTags)
            return paycode['code'] if paycode and paycode['code'] else config.regular_pay_type

        def get_pay_code(entry, allPaycodes, oefTags):
            if entry:
                payCodeUri = rail.find_first_by_attr_and_get_attr(
                    entry['customMetadata'], 'keyUri', 'urn:replicon:object-type-uri:pay-code', 'value.uri')
                if payCodeUri:
                    return rail.find_first_by_attr_and_get_attr(
                        allPaycodes, 'uri', payCodeUri)
                # no matching paycode
                payTypeOefName = Variable.get(
                    config.pay_type_oef_var_name, default_var='Pay Type')
                tagUri = rail.find_first_by_attr_and_get_attr(
                    entry['extensionFieldValues'], 'definition.displayText', payTypeOefName, 'tag.uri')
                if tagUri:
                    oefTag = rail.find_first_by_attr_and_get_attr(
                        oefTags, 'uri', tagUri)
                    if oefTag:
                        return {
                            "code": oefTag['code'],
                            "multiplier": oefTag['description'] if oefTag['description'] else 1.0
                        }
            return None

        def get_reg_hours(time_entries, allPayCodes, oefTags, time_offs):
            totalHours = 0.0
            if allPayCodes:
                for entry in time_entries:
                    if is_project_allocation_type(entry) and entry and entry['interval'] \
                            and is_reg_paycode(entry, allPayCodes, oefTags):
                        totalHours += get_hours(entry['interval'])
            if time_offs:
                for time_off in time_offs:
                    for entry in time_off['entries']:
                        totalHours += get_duration_hours(entry['duration'])
            
            return totalHours

        def get_total_hours(time_entries, allPayCodes, time_offs = None):
            totalHours = 0.0
            if allPayCodes:
                for entry in time_entries:
                    if is_project_allocation_type(entry) and entry and entry['interval']:
                        totalHours += get_hours(entry['interval'])
            if time_offs:
                for time_off in time_offs:
                    for entry in time_off['entries']:
                        totalHours += get_duration_hours(entry['duration'])
            return totalHours

        def get_other_hours(time_entries, allPayCodes, oefTags, time_offs):
            totalHours = 0.0
            for entry in time_entries:
                if is_project_allocation_type(entry) and entry and entry['interval'] \
                        and entry['interval'] and not is_reg_paycode(entry, allPayCodes, oefTags):
                    totalHours += get_hours(entry['interval'])
            return totalHours

        def is_reg_paycode(entry, allPayCodes, oefTags):
            payCode = get_pay_code(entry, allPayCodes, oefTags)
            if payCode:
                if float(payCode['multiplier']) == 1.0:
                    return True
                return False
            return True

        def get_hours(hoursObject):
            if hoursObject.get('hours') is not None:
                raw = hoursObject['hours']['hours'] + hoursObject['hours']['minutes']/60.00 + hoursObject['hours']['seconds']/3600.00
                return round(raw, 2)

            timePair = hoursObject.get('timePair')
            if timePair and timePair['startTime'] and timePair['endTime']:
                endDate = datetime(
                    0, 0, 0, timePair['endTime']['hour'], timePair['endTime']['minute'], timePair['endTime']['second'])
                startDate = datetime(
                    0, 0, 0, timePair['startTime']['hour'], timePair['startTime']['minute'], timePair['startTime']['second'])
                if startDate > endDate:
                    endDate = endDate + timedelta(days=1)
                diff = endDate - startDate
                return round(diff.total_seconds()/3600.00, 2)
            return 0.0

        def get_duration_hours(hoursObject):
            return round(hoursObject['hours'] + hoursObject['minutes']/60.00 + hoursObject['seconds']/3600.00 + hoursObject['milliseconds']/3600000.00 + hoursObject['microseconds']/3600000000.00, 2)


        timesync_error = rail.PythonOperator(
            task_id="timesync_error",
            python_callable=get_export_message
        )

        send_unexpected_error = rail.EmailOperator(
            task_id='send_unexpected_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Timesheet Sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong>
            <br /> <br />Hello, <br /> <br /> {{ result('timesync_error') }}
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_replicon_timesheet

        get_replicon_timesheet >> get_replicon_user_details >> is_costpoint_user
        
        is_costpoint_user >> rail.Label(
            'yes') >> get_replicon_time_entries

        is_costpoint_user >> rail.Label(
            'no') >> catch_error
        
        get_replicon_time_entries >> get_user_role >> is_sync_time_off_bookings
        
        is_sync_time_off_bookings >> rail.Label(
            'yes') >> get_replicon_timeoffs >> get_replicon_time_off_type_details>> is_time_entry_against_project

        is_sync_time_off_bookings >> rail.Label(
            'no') >> is_time_entry_against_project

        is_time_entry_against_project >> rail.Label(
            'yes') >> get_project_entries >> get_project_allocations >> get_allocation_roles >> get_replicon_pay_codes

        is_time_entry_against_project >> rail.Label(
            'no') >> get_replicon_pay_codes

        get_replicon_pay_codes >> \
            get_account_details >> get_replicon_task_details >> get_replicon_project_details >> \
            is_other_source_present 
        
        is_other_source_present >> rail.Label(
            'yes') >> catch_error
        
        is_other_source_present >> rail.Label(
            'no') >> get_division_details
        
        get_division_details >> get_oef_tag_details >> \
            is_use_task_allocation
        
        is_use_task_allocation >> rail.Label(
            'yes') >> get_replicon_task_allocations >> is_sync_to_open_period

        is_use_task_allocation >> rail.Label(
            'no') >> is_sync_to_open_period

        is_sync_to_open_period >> rail.Label('yes') >> get_open_subperiods >> \
            get_replicon_timesheet_period_details >> get_costpoint_timesheet_periods >> \
            get_current_timesheet_period >> get_existing_deltek_timesheet_open_period >> get_existing_deltek_timesheet

        is_sync_to_open_period >> rail.Label(
            'no') >> get_existing_deltek_timesheet

        get_existing_deltek_timesheet >> is_timesheet_available
        
        is_timesheet_available >> rail.Label(
            'yes') >> get_reversing_record_data >> revert_existing_time >> get_replicon_role_details

        is_timesheet_available >> rail.Label(
            'no') >> get_replicon_role_details

        get_replicon_role_details >> get_time_entries_to_sync >> get_costpoint_project_plcs >> verify_costpoint_plc_assignments >> is_missing_plc_assignment

        is_missing_plc_assignment >> rail.Label(
            'yes') >> assign_plc_to_project >> get_costpotint_work_force

        is_missing_plc_assignment >> rail.Label(
            'no') >> get_costpotint_work_force

        get_costpotint_work_force >> verify_costpoint_configurations >> is_costpoint_assignment_required

        is_costpoint_assignment_required >> rail.Label(
            'no') >> push_time_to_costpoint

        is_costpoint_assignment_required >> rail.Label(
            'yes') >> update_assignments_in_costpoint >> push_time_to_costpoint

        push_time_to_costpoint >> is_export_successful

        is_export_successful >> rail.Label(
            'no') >> export_error >> send_error >> catch_error >> timesync_error >> send_unexpected_error >> log_to_sumo
        is_export_successful >> rail.Label('yes') >> catch_error
        return dag


rail.for_each_instance(create_dag)
