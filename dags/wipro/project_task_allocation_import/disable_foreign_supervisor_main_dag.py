import pendulum
from wipro.project_task_allocation_import.utils import request_payload,response_filter
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_foreign_manager_dag_id,
        description=f'Wipro Disable Foreign Manager Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2023, 10, 10),
        schedule_interval=config.disable_foreign_manager_schedule,
        max_active_runs=config.master_max_active_run
    ) as dag:

        get_foreign_manager_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_foreign_manager_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(resp, 'displayText', 'Foreign Managers', 'uri')
        )

        get_all_foreign_supervisors = rail.RepliconServiceOperator(
            task_id='get_all_foreign_supervisors',
            endpoint="/services/UserListService1.svc/GetData",
            data= request_payload.get_foreign_managers_payload,
            data_handler= response_filter.foreign_manager_details
        )

        for_each_user = rail.ForEachOperator(
            task_id='for_each_user',
            items=lambda:  rail.result('get_all_foreign_supervisors'),
            start_task='get_direct_reports',
            end_task='for_each_user_end'
        )

        get_direct_reports = rail.RepliconServiceOperator(
            task_id='get_direct_reports',
            endpoint="/services/UserService1.svc/GetDirectReportsForUser",
            data={
                "userUri": "{{ result('for_each_user').uri }}",
                "userStatusOptionUri": "urn:replicon:user-status-option:include-only-enabled-users"
            }
        )

        get_all_projects_for_user = rail.RepliconServiceOperator(
            task_id='get_all_projects_for_user',
            endpoint="/services/ProjectListService1.svc/GetData",
            data= request_payload.get_projects_for_user_payload,
            data_handler= lambda resp: resp['rows']
        )

        can_disable_user = rail.IfOperator(
            task_id='can_disable_user',
            test=lambda: bool(rail.result('get_direct_reports') or rail.result("get_all_projects_for_user")),
            yes_task="for_each_user_end",
            no_task="disable_login",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('for_each_user').uri }}"
            }
        )

        for_each_user_end = rail.EmptyOperator(
            task_id = 'for_each_user_end'
        )

        get_foreign_manager_employee_type_details >> get_all_foreign_supervisors >> for_each_user

        for_each_user >> get_direct_reports >> get_all_projects_for_user >> can_disable_user

        can_disable_user >> rail.Label(
            "Yes") >> for_each_user_end

        can_disable_user >> rail.Label(
            "No") >> disable_login >> for_each_user_end

        for_each_user >> for_each_user_end

    return dag

rail.for_each_instance(create_dag)
