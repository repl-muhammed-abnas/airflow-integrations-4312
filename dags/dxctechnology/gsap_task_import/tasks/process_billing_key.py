from datetime import timedelta
import rail
from dxctechnology.gsap_task_import.utils import response_filters


def process_billing_key(config, project_type, create_task_dag_id, update_dag_task_id):
    with rail.TaskGroup(group_id="process_each_billing_key", prefix_group_id=False):

        rail.ViewDagRunConfOperator(task_id="view_dag_run")

        get_input_tasks_for_project = rail.QueryCollectionOperator(
            task_id="get_input_tasks_for_project",
            name="all_tasks_for_project",
            query=f"SELECT * FROM {'valid_task_records_for_project' if project_type=='gsap' else 'valid_task_records_for_child_project'} WHERE wbs = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.project_name}}" if project_type=='gsap' else '{{ dag_run.conf.parent_wbs}}'
            }
        )

        get_all_gsap_tasks_for_billing_key = rail.RepliconServiceOperator(
            task_id="get_all_gsap_tasks_for_billing_key",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data={
                "parentUri": "{{dag_run.conf.billingkey_task_uri}}"
            },
            response_filter=response_filters.get_descendant_task_details_filter
        )

        has_any_tasks_present = rail.IfOperator(
            task_id="has_any_tasks_present",
            test="{{result('get_all_gsap_tasks_for_billing_key') | is_truthy}}",
            yes_task="process_gsap_tasks",
            no_task="create_gsap_tasks"
        )

        def get_common_trigger_conf(item, dag_run, is_create=False):
            return {
                "project_type": project_type,
                "file_name": dag_run.conf['file_name'],
                "project_name": dag_run.conf['project_name'],
                "project_uri": dag_run.conf['project_uri'],
                "project_startdate": dag_run.conf['project_startdate'],
                "project_enddate": dag_run.conf['project_enddate'],
                "billingkey_task_name": dag_run.conf['billingkey_task_name'],
                "billingkey_task_uri": dag_run.conf['billingkey_task_uri'],
                "user_list": dag_run.conf['user_list'],
                "task_name": item['task_name'],
                "task_code": item['task_code'],
                "task_start_date": item['task_start_date'],
                "task_end_date": item['task_end_date'],
                "existing_task": None if is_create else rail.find_first_by_attr_and_get_attr(rail.result("get_all_gsap_tasks_for_billing_key"),
                                                                                             'task_name', item['task_name']),
                "task_type_oef_uri": dag_run.conf['task_type_oef_uri'],
                "gsap_task_option_uri": dag_run.conf['gsap_task_option_uri']
            }

        process_gsap_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id="process_gsap_tasks",
            items="{{result('get_input_tasks_for_project')}}",
            trigger_dag_id=update_dag_task_id,
            conf=lambda item, dag_run: get_common_trigger_conf(
                item, dag_run, False),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_process_gsap_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_gsap_tasks',
            dag_runs='{{ result("process_gsap_tasks") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        create_gsap_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id="create_gsap_tasks",
            items="{{result('get_input_tasks_for_project')}}",
            trigger_dag_id=create_task_dag_id,
            conf=lambda item, dag_run: get_common_trigger_conf(
                item, dag_run, True),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_create_gsap_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_gsap_tasks',
            dag_runs='{{ result("create_gsap_tasks") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_input_tasks_for_project >> get_all_gsap_tasks_for_billing_key >> has_any_tasks_present

        has_any_tasks_present >> rail.Label(
            "No") >> create_gsap_tasks >> wait_for_create_gsap_tasks >> finish
        has_any_tasks_present >> rail.Label(
            "Yes") >> process_gsap_tasks >> wait_for_process_gsap_tasks >> finish

        return get_input_tasks_for_project, finish
