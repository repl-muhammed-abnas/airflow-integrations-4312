from datetime import timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.task.create_group_level_1 import create_group_level1_task_group
from technicolorg3.user_import.utils import request_payload
from technicolorg3.user_import.utils.python_callable_method import get_dag_run_conf, get_downstreamtasks_error, get_groupuri_from_mapper

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


# pylint:disable = too-many-statements
def create_groups_add_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_groups_add_{config.instance}',
        description=f'Technicolor User Import Add Groups {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_groups_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_costcenter'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_costcenter',
            end_task='catch_group_error',
        )

        process_costcenter = rail.EmptyOperator(
            task_id='process_costcenter'
        )

        is_create_cost_center = rail.IfOperator(
            task_id='is_create_cost_center',
            test="{{ dag_run.conf.type == 'costcenter' }}",
            yes_task='create_costcenter_group',
            no_task='process_department_group'
        )

        create_costcenter_group = rail.RepliconServiceOperator(
            task_id='create_costcenter_group',
            endpoint='services/{{ dag_run.conf.dws_service_name }}Service1.svc/Create{{ dag_run.conf.dws_service_name }}OrApplyModification',
            data=request_payload.create_costcenter_payload
        )

        process_department_group = rail.EmptyOperator(
            task_id='process_department_group'
        )

        is_create_department_group = rail.IfOperator(
            task_id='is_create_department_group',
            test="{{ dag_run.conf.type == 'department' }}",
            yes_task='get_department_params',
            no_task='process_service_center'
        )

        (get_department_params, is_department_hierarchy_exception, is_department_hierarchy_level1,
         write_department_log) = create_group_level1_task_group('Department')

        process_other_levels_department = rail.TriggerDagRunForEachItemOperator(
            task_id='process_other_levels_department',
            retries=0,
            items=lambda: rail.result('get_department_params')[
                'required_name'].split('|'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_department_level_{config.instance}',
            conf=request_payload.get_process_other_dept_levels_conf
        )

        wait_other_levels_department = rail.WaitForDagRunsSensor(
            task_id='wait_other_levels_department',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_other_levels_department") }}',
        )

        gather_departmentlevels_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_departmentlevels_error',
            dag_runs="{{ result('process_other_levels_department') }}",
            dagrun_task_id='catch_departmentgroup_level_error',
            flatten=True
        )

        is_departmentlevel_error = rail.IfOperator(
            task_id='is_departmentlevel_error',
            test="{{ result('gather_departmentlevels_error') | length > 0 }}",
            yes_task='fail_dag_departmentlevel_error',
            no_task='catch_group_error'
        )

        fail_dag_departmentlevel_error = rail.FailOperator(
            task_id='fail_dag_departmentlevel_error',
            message="{{ result('gather_departmentlevels_error') | map_to_attr('error') | join('|') }}"
        )

        process_service_center = rail.EmptyOperator(
            task_id='process_service_center'
        )

        is_create_service_center = rail.IfOperator(
            task_id='is_create_service_center',
            test="{{ dag_run.conf.type == 'servicecenter' }}",
            yes_task='search_gmbh_servicecenter_entries_in_replicon_lvl1',
            no_task='process_location'
        )

        def do_filter_servicecenterlog(log):
            dag_run_conf = get_dag_run_conf()
            return log['properties']['fullpath'] == dag_run_conf[
                'servicecenter'] and log['properties']['type'] == dag_run_conf['type']
        search_gmbh_servicecenter_entries_in_replicon_lvl1 = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_servicecenter_entries_in_replicon_lvl1',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_servicecenterlog
        )

        is_servicecenter_lvl1_group_present = rail.IfOperator(
            task_id='is_servicecenter_lvl1_group_present',
            test="{{ result('search_gmbh_servicecenter_entries_in_replicon_lvl1', 'length') > 0 }}",
            yes_task='catch_group_error',
            no_task='get_servicecenter_params'
        )

        (get_servicecenter_params, is_servicecenter_hierarchy_exception, is_servicecenter_hierarchy_level1,
         write_servicecenter_log) = create_group_level1_task_group('ServiceCenter')

        process_other_levels_servicecenter = rail.EmptyOperator(
            task_id='process_other_levels_servicecenter'
        )

        def do_filter_servicecenter_entries(log):
            dag_run_conf = get_dag_run_conf()
            parent_servicecenter_fullpath = rail.smartjoin_by_delim(
                dag_run_conf['servicecenter'].split('|')[:-1], '|')
            return log['properties']['fullpath'] == parent_servicecenter_fullpath and log[
                'properties']['type'] == dag_run_conf['type']
        search_gmbh_servicecentergroup_lvl1_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_servicecentergroup_lvl1_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_servicecenter_entries
        )

        get_servicecenter = rail.PythonOperator(
            task_id='get_servicecenter',
            python_callable=get_groupuri_from_mapper,
            op_args=[
                "{{ result('search_gmbh_servicecentergroup_lvl1_entries') }}"]
        )

        is_servicecenter_present = rail.IfOperator(
            task_id='is_servicecenter_present',
            test="{{ result('get_servicecenter') | is_truthy }}",
            yes_task='create_servicecenter_or_applymodifications_lvl3',
            no_task='search_gmbh_servicecentergroup_lvl2_parent'
        )

        def do_filter_parent_servicecenter_entries(log):
            dag_run_conf = get_dag_run_conf()
            parent_servicecenter_lvl2 = rail.smartjoin_by_delim(rail.smartjoin_by_delim(
                dag_run_conf['servicecenter'].split('|')[:-1], '|').split('|')[:-1], '|')
            return log['properties']['fullpath'] == parent_servicecenter_lvl2 and log[
                'properties']['type'] == dag_run_conf['type']

        search_gmbh_servicecentergroup_lvl2_parent = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_servicecentergroup_lvl2_parent',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_parent_servicecenter_entries
        )

        get_servicecenter_lvl2_parent = rail.PythonOperator(
            task_id='get_servicecenter_lvl2_parent',
            python_callable=get_groupuri_from_mapper,
            op_args=[
                "{{ result('search_gmbh_servicecentergroup_lvl2_parent') }}"]
        )

        is_servicecenter_lvl2_parent_present = rail.IfOperator(
            task_id='is_servicecenter_lvl2_parent_present',
            test="{{ result('get_servicecenter_lvl2_parent') | is_falsy and \
                result('get_servicecenter_params').required_level == 3 }}",
            yes_task='create_servicecenter_or_applymodifications_lvl1',
            no_task='create_servicecenter_or_applymodifications_lvl2'
        )

        create_servicecenter_or_applymodifications_lvl1 = rail.RepliconServiceOperator(
            task_id='create_servicecenter_or_applymodifications_lvl1',
            endpoint='services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification',
            data=request_payload.get_create_servicecenter_lvl1_payload
        )

        write_level1_servicecenter = rail.WriteLogOperator(
            task_id='write_level1_servicecenter',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add service center level1 to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_servicecenter_or_applymodifications_lvl1')['displayText'],
                'uri': rail.result('create_servicecenter_or_applymodifications_lvl1')['uri'],
                'fullpath': rail.smartjoin_by_delim(rail.smartjoin_by_delim(
                    dag_run.conf['servicecenter'].split('|')[:-1], '|').split('|')[:-1], '|'),
                'type': 'servicecenter'
            }
        )

        create_servicecenter_or_applymodifications_lvl2 = rail.RepliconServiceOperator(
            task_id='create_servicecenter_or_applymodifications_lvl2',
            endpoint='services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification',
            data=request_payload.get_create_servicecenter_lvl2_payload
        )

        write_level2_servicecenter = rail.WriteLogOperator(
            task_id='write_level2_servicecenter',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add service center level2 to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_servicecenter_or_applymodifications_lvl2')['displayText'],
                'uri': rail.result('create_servicecenter_or_applymodifications_lvl2')['uri'],
                'fullpath': rail.smartjoin_by_delim(
                    dag_run.conf['servicecenter'].split('|')[:-1], '|'),
                'type': 'servicecenter'
            }
        )

        create_servicecenter_or_applymodifications_lvl3 = rail.RepliconServiceOperator(
            task_id='create_servicecenter_or_applymodifications_lvl3',
            endpoint='services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification',
            data=request_payload.get_create_servicecenter_lvl3_payload
        )

        write_level3_servicecenter = rail.WriteLogOperator(
            task_id='write_level3_servicecenter',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add service center level3 to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_servicecenter_or_applymodifications_lvl3')['displayText'],
                'uri': rail.result('create_servicecenter_or_applymodifications_lvl3')['uri'],
                'fullpath': dag_run.conf['servicecenter'],
                'type': 'servicecenter'
            }
        )

        process_location = rail.EmptyOperator(
            task_id='process_location'
        )

        is_create_location = rail.IfOperator(
            task_id='is_create_location',
            test="{{ dag_run.conf.type == 'location' }}",
            yes_task='get_location_params',
            no_task='process_division'
        )

        (get_location_params, is_location_hierarchy_exception, is_location_hierarchy_level1,
         write_location_log) = create_group_level1_task_group('Location')

        process_other_levels_location = rail.EmptyOperator(
            task_id='process_other_levels_location'
        )

        def do_filter_location_entries(log):
            dag_run_conf = get_dag_run_conf()
            return log['properties']['fullpath'] == dag_run_conf['location'].split('|')[0] and log[
                'properties']['type'] == dag_run_conf['type']
        search_gmbh_location_lvl1_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_location_lvl1_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_location_entries
        )

        get_location = rail.PythonOperator(
            task_id='get_location',
            python_callable=get_groupuri_from_mapper,
            op_args=["{{ result('search_gmbh_location_lvl1_entries') }}"]
        )

        is_location_present = rail.IfOperator(
            task_id='is_location_present',
            test="{{ result('get_location') | is_truthy }}",
            yes_task='create_location_or_applymodifications_lvl2',
            no_task='create_location_or_applymodifications_lvl1'
        )

        create_location_or_applymodifications_lvl1 = rail.RepliconServiceOperator(
            task_id='create_location_or_applymodifications_lvl1',
            endpoint='services/LocationService1.svc/CreateLocationOrApplyModification',
            data=request_payload.get_create_location_lvl1_payload
        )

        write_level1_location = rail.WriteLogOperator(
            task_id='write_level1_location',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add location level1 to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_location_or_applymodifications_lvl1')['displayText'],
                'uri': rail.result('create_location_or_applymodifications_lvl1')['uri'],
                'fullpath': dag_run.conf['location'].split('|')[0],
                'type': 'location'
            }
        )

        create_location_or_applymodifications_lvl2 = rail.RepliconServiceOperator(
            task_id='create_location_or_applymodifications_lvl2',
            endpoint='services/LocationService1.svc/CreateLocationOrApplyModification',
            data=request_payload.get_create_location_lvl2_payload
        )

        write_level2_location = rail.WriteLogOperator(
            task_id='write_level2_location',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add location level2 to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_location_or_applymodifications_lvl2')['displayText'],
                'uri': rail.result('create_location_or_applymodifications_lvl2')['uri'],
                'fullpath': dag_run.conf['location'],
                'type': 'location'
            }
        )

        process_division = rail.EmptyOperator(
            task_id='process_division'
        )

        is_create_division = rail.IfOperator(
            task_id='is_create_division',
            test="{{ dag_run.conf.type == 'division' }}",
            yes_task='create_division_group',
            no_task='catch_group_error'
        )

        create_division_group = rail.RepliconServiceOperator(
            task_id='create_division_group',
            endpoint='services/{{ dag_run.conf.dws_service_name }}Service1.svc/Create{{ dag_run.conf.dws_service_name }}OrApplyModification',
            data=request_payload.create_division_payload
        )

        catch_group_error = rail.PythonOperator(
            task_id='catch_group_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_group_error

        can_run_batch_task >> rail.Label(
            'No') >> process_costcenter

        process_costcenter >> is_create_cost_center

        is_create_cost_center >> rail.Label(
            'Yes') >> create_costcenter_group

        is_create_cost_center >> rail.Label(
            'No') >> process_department_group >> is_create_department_group

        is_create_department_group >> rail.Label(
            'Yes') >> get_department_params

        is_department_hierarchy_level1 >> rail.Label(
            'No') >> process_other_levels_department >> wait_other_levels_department >> \
            gather_departmentlevels_error >> is_departmentlevel_error

        is_departmentlevel_error >> rail.Label(
            'Yes') >> fail_dag_departmentlevel_error

        is_departmentlevel_error >> rail.Label(
            'No') >> catch_group_error

        is_create_department_group >> rail.Label(
            'No') >> process_service_center >> is_create_service_center

        is_create_service_center >> rail.Label(
            'Yes') >> search_gmbh_servicecenter_entries_in_replicon_lvl1 >> is_servicecenter_lvl1_group_present

        is_servicecenter_lvl1_group_present >> rail.Label(
            'Yes') >> catch_group_error

        is_servicecenter_lvl1_group_present >> rail.Label(
            'No') >> get_servicecenter_params

        is_servicecenter_hierarchy_level1 >> rail.Label(
            'No') >> process_other_levels_servicecenter

        process_other_levels_servicecenter >> search_gmbh_servicecentergroup_lvl1_entries >> get_servicecenter >> \
            is_servicecenter_present

        is_servicecenter_present >> rail.Label(
            'No') >> search_gmbh_servicecentergroup_lvl2_parent >> get_servicecenter_lvl2_parent >> is_servicecenter_lvl2_parent_present

        is_servicecenter_lvl2_parent_present >> rail.Label(
            'Yes') >> create_servicecenter_or_applymodifications_lvl1 >> write_level1_servicecenter >> create_servicecenter_or_applymodifications_lvl2

        is_servicecenter_lvl2_parent_present >> rail.Label(
            'No') >> create_servicecenter_or_applymodifications_lvl2

        create_servicecenter_or_applymodifications_lvl2 >> write_level2_servicecenter >> create_servicecenter_or_applymodifications_lvl3

        is_servicecenter_present >> rail.Label(
            'Yes') >> create_servicecenter_or_applymodifications_lvl3 >> write_level3_servicecenter

        is_create_service_center >> rail.Label(
            'No') >> process_location >> is_create_location

        is_create_location >> rail.Label(
            'Yes') >> get_location_params

        is_location_hierarchy_level1 >> rail.Label(
            'No') >> process_other_levels_location

        process_other_levels_location >> search_gmbh_location_lvl1_entries >> get_location >> is_location_present

        is_location_present >> rail.Label(
            'No') >> create_location_or_applymodifications_lvl1 >> write_level1_location >> create_location_or_applymodifications_lvl2

        is_location_present >> rail.Label(
            'Yes') >> create_location_or_applymodifications_lvl2 >> write_level2_location

        is_create_location >> rail.Label(
            'No') >> process_division >> is_create_division

        is_create_division >> rail.Label(
            'Yes') >> create_division_group

        is_create_division >> rail.Label(
            'No') >> catch_group_error

        create_costcenter_group >> rail.Label(
            'On Error') >> catch_group_error

        is_department_hierarchy_exception >> rail.Label(
            'Yes') >> catch_group_error

        write_department_log >> rail.Label(
            'On Error') >> catch_group_error

        fail_dag_departmentlevel_error >> rail.Label(
            'On Error') >> catch_group_error

        is_servicecenter_hierarchy_exception >> rail.Label(
            'Yes') >> catch_group_error

        write_servicecenter_log >> rail.Label(
            'On Error') >> catch_group_error

        write_level3_servicecenter >> rail.Label(
            'On Error') >> catch_group_error

        is_location_hierarchy_exception >> rail.Label(
            'Yes') >> catch_group_error

        write_location_log >> rail.Label(
            'On Error') >> catch_group_error

        write_level2_location >> rail.Label(
            'On Error') >> catch_group_error

        create_division_group >> rail.Label(
            'On Error') >> catch_group_error

        catch_group_error >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_groups_add_child_dag)
