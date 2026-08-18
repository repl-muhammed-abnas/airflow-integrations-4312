import rail
from pwcglobal.user_import_australia import request_payload, custom_methods


def create_add_activities_task(user_uri=None):
    with rail.TaskGroup(group_id="add_activities_task", prefix_group_id=False):
        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
        )

        def employee_type_check(dag_run, check_type):
            employee_type = dag_run.conf['employee_type'] + \
                ' - ' + dag_run.conf['time_type']
            if check_type == "regular_fixed":
                return employee_type in ['Regular - Full time', 'Regular - Part time', 'Fixed Term - Full time', 'Fixed Term - Part time']
            if check_type == "non_salaried_self_employed":
                return employee_type == "Non-Salaried / Self-Employed - Full time"

            return False

        is_non_salaried_self_employed = rail.IfOperator(
            task_id="is_non_salaried_self_employed",
            test=lambda dag_run: employee_type_check(
                dag_run, "non_salaried_self_employed"),
            yes_task="put_activity_assignment_for_user",
            no_task="is_regular_fixed_term"
        )

        put_activity_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_activity_assignment_for_user",
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: request_payload.get_activity_assignment_payload(
                dag_run, "non_salaried_self_employed", user_uri)
        )

        is_regular_fixed_term = rail.IfOperator(
            task_id="is_regular_fixed_term",
            test=lambda dag_run: employee_type_check(dag_run, "regular_fixed"),
            yes_task="put_activity_assignment2_for_user",
            no_task="get_entries_from_mapper"
        )
        put_activity_assignment2_for_user = rail.RepliconServiceOperator(
            task_id="put_activity_assignment2_for_user",
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: request_payload.get_activity_assignment_payload(
                dag_run, "regular_fixed_term", user_uri)
        )

        get_entries_from_mapper = rail.PythonOperator(
            task_id="get_entries_from_mapper",
            python_callable=custom_methods.get_entries_from_user_mapper
        )
        get_all_activities >> is_non_salaried_self_employed >> rail.Label(
            "Yes") >> put_activity_assignment_for_user >> get_entries_from_mapper
        is_non_salaried_self_employed >> rail.Label("No") >> is_regular_fixed_term >> rail.Label(
            "Yes") >> put_activity_assignment2_for_user >> get_entries_from_mapper
        is_regular_fixed_term >> rail.Label("No") >> get_entries_from_mapper

        return get_all_activities, get_entries_from_mapper
