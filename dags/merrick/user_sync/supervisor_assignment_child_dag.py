from datetime import timedelta
from datetime import datetime
import itertools
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.supervisor_assignment_dag_id,
        description=f'merrick_supervisor_assignment_{config.instance}',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
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
            no_task='if_supervisor_users_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_supervisor_users_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_supervisor_users_present = rail.IfOperator(
            task_id='if_supervisor_users_present',
            test="{{ dag_run.conf.supervisorassignment | length > 0 }}",
            yes_task="get_super_users_permission",
            no_task="catch_error",
        )

        def get_superuser_uris(dag_run):
            supervisor_user_uris = []
            super_user_history = dag_run.conf['supervisorassignment']
            for superuser in super_user_history:
                if superuser['superuseruri'] and superuser['superuseruri'] not in supervisor_user_uris:
                    supervisor_user_uris.append(superuser['superuseruri'])
            return supervisor_user_uris

        get_super_users_permission = rail.RepliconServiceOperator(
            task_id='get_super_users_permission',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data=lambda dag_run: {
                "userUris": get_superuser_uris(dag_run)
            }
        )

        foreach_supervisor_assignment = rail.ForEachOperator(
            task_id='foreach_supervisor_assignment',
            items="{{ dag_run.conf.supervisorassignment | to_json }}",
            start_task='if_supervisor_assignment_present',
            end_task='foreach_supervisor_assignment_end'
        )

        def is_not_supervisor_permission_present():
            supervisor_permission = list(filter(lambda x: x['user']['uri'] == rail.result(
                'foreach_supervisor_assignment')['superuseruri'], rail.result('get_super_users_permission')))
            permissionset = rail.find_first_by_attr_and_get_attr(
                supervisor_permission, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet')
            if permissionset or not rail.result('foreach_supervisor_assignment')['superuseruri']:
                return False
            return True

        if_supervisor_assignment_present = rail.IfOperator(
            task_id='if_supervisor_assignment_present',
            test=is_not_supervisor_permission_present,
            yes_task="put_permissions_user",
            no_task="foreach_supervisor_assignment_end",
        )

        put_permissions_user = rail.RepliconServiceOperator(
            task_id='put_permissions_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                'userUri': rail.result('foreach_supervisor_assignment')['superuseruri'],
                "permissionSetUri": dag_run.conf['supervisorpermissionuri']
            }
        )

        foreach_supervisor_assignment_end = rail.EmptyOperator(
            task_id='foreach_supervisor_assignment_end',
        )

        def add_supervisor_history(dag_run):
            supervisor_history = []
            for usersuper in dag_run.conf['supervisorassignment']:
                effective_date = datetime.strptime(
                    usersuper['effectivedate'], config.costpoint_date_format) if usersuper['effectivedate'] else None
                super_user_history = dag_run.conf['supervisorassignment']
                supervisor_uri = rail.find_first_by_attr_and_get_attr(
                    super_user_history, 'supervisor', usersuper['supervisor'], 'superuseruri')
                if effective_date and supervisor_uri:
                    supervisor_history.append({
                        "supervisor": {
                            "uri": supervisor_uri,
                            "loginName": null,
                            "employeeId": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": {
                            "year": effective_date.year,
                            "month": effective_date.month,
                            "day": effective_date.day
                        }
                    })
            if supervisor_history:
                return supervisor_history
            return null

        put_supervisor_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": add_supervisor_history(dag_run)
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Add/Update",
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
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_supervisor_users_present
        if_supervisor_users_present >> rail.Label('No') >> catch_error
        if_supervisor_users_present >> rail.Label('Yes') >> get_super_users_permission >> \
            foreach_supervisor_assignment >> \
            if_supervisor_assignment_present
        if_supervisor_assignment_present >> rail.Label(
            'No') >> foreach_supervisor_assignment_end
        if_supervisor_assignment_present >> rail.Label(
            'Yes') >> put_permissions_user >> foreach_supervisor_assignment_end
        foreach_supervisor_assignment >> foreach_supervisor_assignment_end >> \
            put_supervisor_assignment_schedule >> catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
