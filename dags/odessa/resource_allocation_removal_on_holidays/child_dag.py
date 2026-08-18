import rail
from odessa.resource_allocation_removal_on_holidays.utils import request_payload,python_callable
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'odessa_remove_allocation_on_holiday_dates_child_{config.instance}',
        description=f'Odessa_remove_allocation_on_holiday_dates_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")


        get_resource_allocation_details = rail.RepliconServiceOperator(
            task_id='get_resource_allocation_details',
            endpoint="/services/ResourceService1.svc/GetResourceAllocationSummary",
            data=request_payload.get_report_payload
        )


        if_project_displaytext_present = rail.IfOperator(
            task_id='if_project_displaytext_present',
            test=lambda: bool(python_callable.get_project_allocation_data(
                rail.result('get_resource_allocation_details'))),
            yes_task="for_each_project_allocation",
            no_task="finish",
        )

        for_each_project_allocation = rail.ForEachOperator(
            task_id='for_each_project_allocation',
            items="{{ result('get_resource_allocation_details').projectsAllocatedTo | to_json}}",
            start_task='put_project_resource_allocation_17',
            end_task='for_each_project_allocation_end'
        )

        put_project_resource_allocation_17 = rail.RepliconServiceOperator(
            task_id='put_project_resource_allocation_17',
            endpoint="/services/ResourceService1.svc/PutProjectResourceAllocation",
            data=request_payload.get_project_resource_allocation_payload
        )

        for_each_project_allocation_end = rail.EmptyOperator(
            task_id='for_each_project_allocation_end'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log= "{{ dag_run.conf.child_log }}",
            message="Adding the multiple entries",
            severity='Success',
            properties={
                'loginname':  "{{dag_run.conf.loginname}}",
                'useruri': "{{dag_run.conf.resourceUri}}",
                'holidaycalendar': "{{dag_run.conf.holidaycalendarname}}",
                'holidaydate': "{{dag_run.conf.holidaydate }}",
                'status': 'Success',
                'holidayname': "{{dag_run.conf.holidayname}}"
            }
        )



        log_ignored_entries = rail.WriteLogOperator(
            task_id='log_ignored_entries',
            log= "{{ dag_run.conf.child_log }}",
            message="Adding the multiple entries",
            severity='Ignored',
            properties={
                'loginname':  "{{dag_run.conf.loginname}}",
                'useruri': "{{dag_run.conf.resourceUri}}",
                'holidaycalendar': "{{dag_run.conf.holidaycalendarname}}",
                'holidaydate': "{{dag_run.conf.holidaydate }}",
                'status': 'Ignored',
                'holidayname': "{{dag_run.conf.holidayname}}"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log= "{{ dag_run.conf.child_log }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'loginname':  "{{dag_run.conf.loginname}}",
                'useruri': "{{dag_run.conf.resourceUri}}",
                'holidaycalendar': "{{dag_run.conf.holidaycalendarname}}",
                'holidaydate': "{{dag_run.conf.holidaydate }}",
                'status': 'Error',
                'holidayname': "{{dag_run.conf.holidayname}}"
            }
        )

        get_resource_allocation_details >>if_project_displaytext_present
        if_project_displaytext_present >> rail.Label(
            'No') >> finish >> log_ignored_entries >> catch_and_log_errors
        if_project_displaytext_present >> rail.Label(
            'Yes') >> for_each_project_allocation >> put_project_resource_allocation_17
        put_project_resource_allocation_17 >> for_each_project_allocation_end >> log_success_entries >> catch_and_log_errors
        for_each_project_allocation >> for_each_project_allocation_end
        return dag


rail.for_each_instance(create_dag)
