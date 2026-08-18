from datetime import timedelta
import itertools
from uuid import uuid4
from airflow.models import Variable
import rail

from deltek_vantagepoint.user_sync.utils.python_callable_method import page_handler
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_process_supervisor_assignment_child_{config.instance}',
        description='Handles Supervisor Assignment for Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_supervisor'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'].lower() == loginname.lower(), map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri'],
                'supervisor': row['cells'][4]['uri'] if 'uri' in row['cells'][4] else None
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_supervisor = rail.RepliconServicePageOperator(
            task_id='search_supervisor',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled',
                    'urn:replicon:user-list-column:supervisor'
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": dag_run.conf['supervisor']
                        }
                    }
                },
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['supervisor'])
        )

        if_supervisor_user_present_and_enabled = rail.IfOperator(
            task_id='if_supervisor_user_present_and_enabled',
            test=lambda: rail.result('search_supervisor') and rail.result(
                'search_supervisor').get('useruri') and rail.result('search_supervisor').get('status') == 'True',
            yes_task='search_supervisor_permission',
            no_task='add_log_supervisor_not_assigned'
        )

        search_supervisor_permission = rail.RepliconServiceOperator(
            task_id='search_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                "userUri": rail.result('search_supervisor').get('useruri')
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision')
        )

        if_supervisor_permission_not_present = rail.IfOperator(
            task_id='if_supervisor_permission_not_present',
            test=lambda dag_run: not rail.result(
                'search_supervisor_permission'),
            yes_task='assign_supervisor_permission',
            no_task='assign_supervisor'
        )

        assign_supervisor_permission = rail.RepliconServiceOperator(
            task_id='assign_supervisor_permission',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                'userUri': rail.result('search_supervisor').get('useruri'),
                "permissionSetUri": dag_run.conf['supervisorpermissionuri']
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint='services/ImportService2.svc/CreateUserOrApplyModifications',
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "supervisorSchedule": [
                        {
                            "dateRange": {
                                "startDate": dag_run.conf['effectivedate'] if dag_run.conf['currentsupervisor'] else null
                            },
                            "item": {
                                "uri": rail.result('search_supervisor').get('useruri'),
                            }
                        }
                    ]
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        add_log_supervisor_not_assigned = rail.WriteLogOperator(
            task_id='add_log_supervisor_not_assigned',
            message="na",
            severity="Error/Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Supervisor Assignment",
                "status": "Error",
                "reason": "Supervisor not assigned because it is either not present or is disabled in Replicon"
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error/Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Supervisor Assignment",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> search_supervisor >> if_supervisor_user_present_and_enabled
        if_supervisor_user_present_and_enabled >> rail.Label(
            'Yes') >> search_supervisor_permission >> if_supervisor_permission_not_present
        if_supervisor_permission_not_present >> rail.Label(
            'Yes') >> assign_supervisor_permission >> assign_supervisor
        if_supervisor_permission_not_present >> rail.Label(
            'No') >> assign_supervisor >> catch_error
        if_supervisor_user_present_and_enabled >> rail.Label(
            'No') >> add_log_supervisor_not_assigned >> catch_error >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
