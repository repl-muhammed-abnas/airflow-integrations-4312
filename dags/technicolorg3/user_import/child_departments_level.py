from datetime import timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.utils.python_callable_method import do_filter_departmentlog, do_filter_parent_departmentlog, get_downstreamtasks_error
from technicolorg3.user_import.utils import request_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


# pylint:disable=too-many-statements
def create_departmentgroup_level_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_department_level_{config.instance}',
        description=f'Technicolor User Import Departments Level {config.instance}',
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
            no_task='process_firstlevel'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_firstlevel',
            end_task='catch_departmentgroup_level_error',
        )

        process_firstlevel = rail.EmptyOperator(
            task_id='process_firstlevel'
        )

        is_first_level = rail.IfOperator(
            task_id='is_first_level',
            test=lambda dag_run: dag_run.conf['level'] == 1,
            yes_task='search_gmbh_departmentgroup_lvl1_entries',
            no_task='process_other_levels'
        )

        search_gmbh_departmentgroup_lvl1_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_departmentgroup_lvl1_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_departmentlog
        )

        is_replicon_lvl1_group_present = rail.IfOperator(
            task_id='is_replicon_lvl1_group_present',
            test="{{ result('search_gmbh_departmentgroup_lvl1_entries', 'length') > 0 }}",
            yes_task='process_other_levels',
            no_task='create_level2_departmentgroup_in_replicon'
        )

        create_level2_departmentgroup_in_replicon = rail.RepliconServiceOperator(
            task_id='create_level2_departmentgroup_in_replicon',
            endpoint='services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=request_payload.create_departmentgroup_level2_payload
        )

        write_level2_departmentgroup = rail.WriteLogOperator(
            task_id='write_level2_departmentgroup',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add department level2 to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_level2_departmentgroup_in_replicon')['displayText'],
                'uri': rail.result('create_level2_departmentgroup_in_replicon')['uri'],
                'fullpath': dag_run.conf['required_department_fullpath'],
                'type': 'department'
            }
        )

        process_other_levels = rail.EmptyOperator(
            task_id='process_other_levels'
        )

        is_not_first_and_last_level = rail.IfOperator(
            task_id='is_not_first_and_last_level',
            test=lambda dag_run: dag_run.conf['level'] not in (1, 6),
            yes_task='search_gmbh_departmentgroup_entries',
            no_task='process_last_level'
        )

        search_gmbh_departmentgroup_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_departmentgroup_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_departmentlog
        )

        is_entries_present = rail.IfOperator(
            task_id='is_entries_present',
            test="{{ result('search_gmbh_departmentgroup_entries', 'length') > 0 }}",
            yes_task='process_last_level',
            no_task='search_gmbh_departmentgroup_parent_department_entries'
        )

        search_gmbh_departmentgroup_parent_department_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_departmentgroup_parent_department_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_parent_departmentlog
        )

        create_specificlevel_departmentgroup_in_replicon = rail.RepliconServiceOperator(
            task_id='create_specificlevel_departmentgroup_in_replicon',
            endpoint='services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=request_payload.create_departmentgroup_specificlevel_payload
        )

        write_specificlevel_departmentgroup = rail.WriteLogOperator(
            task_id='write_specificlevel_departmentgroup',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add specific level to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_specificlevel_departmentgroup_in_replicon')['displayText'],
                'uri': rail.result('create_specificlevel_departmentgroup_in_replicon')['uri'],
                'fullpath': dag_run.conf['required_department_fullpath'],
                'type': 'department'
            }
        )

        process_last_level = rail.EmptyOperator(
            task_id='process_last_level'
        )

        search_gmbh_departmentgroup_last_lvl_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_departmentgroup_last_lvl_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_departmentlog
        )

        is_last_lvl_entries_present = rail.IfOperator(
            task_id='is_last_lvl_entries_present',
            test="{{ result('search_gmbh_departmentgroup_last_lvl_entries', 'length') > 0 }}",
            yes_task='catch_departmentgroup_level_error',
            no_task='search_gmbh_departmentgroup_last_lvl_parent_department_entries'
        )

        search_gmbh_departmentgroup_last_lvl_parent_department_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_departmentgroup_last_lvl_parent_department_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_parent_departmentlog
        )

        should_create_mikros_department_group = rail.IfOperator(
            task_id='should_create_mikros_department_group',
            test="{{ dag_run.conf.required_department_name == 'MikrosMPC' }}",
            yes_task='search_gmbh_departmentgroup_last_lvl_mikros_department_entries',
            no_task='create_lastlevel_departmentgroup_in_replicon'
        )

        search_gmbh_departmentgroup_last_lvl_mikros_department_entries = rail.FilterLogEntriesOperator(
            task_id='search_gmbh_departmentgroup_last_lvl_mikros_department_entries',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            filter_callable=do_filter_parent_departmentlog
        )

        is_parent_group_entries_present = rail.IfOperator(
            task_id='is_parent_group_entries_present',
            test="{{ result('search_gmbh_departmentgroup_last_lvl_mikros_department_entries', 'length') > 0 }}",
            yes_task='create_lastlevel_departmentgroup_in_replicon',
            no_task='create_lastlevel_mikros_departmentgroup_in_replicon'
        )

        create_lastlevel_mikros_departmentgroup_in_replicon = rail.RepliconServiceOperator(
            task_id='create_lastlevel_mikros_departmentgroup_in_replicon',
            endpoint='services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=request_payload.create_mikros_departmentgroup_lastlevel_payload
        )

        write_lastlevel_mikros_departmentgroup = rail.WriteLogOperator(
            task_id='write_lastlevel_mikros_departmentgroup',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add last level mikros department to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_lastlevel_mikros_departmentgroup_in_replicon')['displayText'],
                'uri': rail.result('create_lastlevel_mikros_departmentgroup_in_replicon')['uri'],
                'fullpath': f"{dag_run.conf['parent_department_fullpath']}/MPC - Advertising",
                'type': 'department'
            }
        )

        create_lastlevel_departmentgroup_in_replicon = rail.RepliconServiceOperator(
            task_id='create_lastlevel_departmentgroup_in_replicon',
            endpoint='services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification',
            data=request_payload.create_departmentgroup_lastlevel_payload
        )

        write_lastlevel_departmentgroup = rail.WriteLogOperator(
            task_id='write_lastlevel_departmentgroup',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message='add last level department to gmbh log',
            properties=lambda dag_run: {
                'name': rail.result('create_lastlevel_departmentgroup_in_replicon')['displayText'],
                'uri': rail.result('create_lastlevel_departmentgroup_in_replicon')['uri'],
                # pylint: disable=line-too-long
                'fullpath': f"{dag_run.conf['required_department_fullpath']}/MPC - Advertising/{dag_run.conf['required_department_name']}" if dag_run.conf['required_department_name'] == 'MikrosMPC' else dag_run.conf['required_department_fullpath'],
                'type': 'department'
            }
        )

        catch_departmentgroup_level_error = rail.PythonOperator(
            task_id='catch_departmentgroup_level_error',
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
            'Yes') >> batch_task >> rail.Label(
                'Error') >> catch_departmentgroup_level_error

        can_run_batch_task >> rail.Label(
            'No') >> process_firstlevel

        process_firstlevel >> is_first_level

        is_first_level >> rail.Label(
            'Yes') >> search_gmbh_departmentgroup_lvl1_entries >> is_replicon_lvl1_group_present

        is_replicon_lvl1_group_present >> rail.Label(
            'No') >> create_level2_departmentgroup_in_replicon >> write_level2_departmentgroup >> rail.Label(
                'Error') >> catch_departmentgroup_level_error

        is_replicon_lvl1_group_present >> rail.Label(
            'Yes') >> process_other_levels

        is_first_level >> rail.Label(
            'No') >> process_other_levels

        process_other_levels >> is_not_first_and_last_level

        is_not_first_and_last_level >> rail.Label(
            'Yes') >> search_gmbh_departmentgroup_entries >> is_entries_present

        is_entries_present >> rail.Label(
            'No') >> search_gmbh_departmentgroup_parent_department_entries >> create_specificlevel_departmentgroup_in_replicon >> \
            write_specificlevel_departmentgroup >> rail.Label(
                'Error') >> catch_departmentgroup_level_error

        is_entries_present >> rail.Label(
            'Yes') >> process_last_level

        is_not_first_and_last_level >> rail.Label(
            'No') >> process_last_level

        process_last_level >> search_gmbh_departmentgroup_last_lvl_entries >> is_last_lvl_entries_present

        is_last_lvl_entries_present >> rail.Label(
            'Error') >> catch_departmentgroup_level_error

        is_last_lvl_entries_present >> rail.Label(
            'No') >> search_gmbh_departmentgroup_last_lvl_parent_department_entries >> should_create_mikros_department_group

        should_create_mikros_department_group >> rail.Label(
            'Yes') >> search_gmbh_departmentgroup_last_lvl_mikros_department_entries >> is_parent_group_entries_present

        is_parent_group_entries_present >> rail.Label(
            'No') >> create_lastlevel_mikros_departmentgroup_in_replicon >> write_lastlevel_mikros_departmentgroup >> \
            create_lastlevel_departmentgroup_in_replicon

        is_parent_group_entries_present >> rail.Label(
            'Yes') >> create_lastlevel_departmentgroup_in_replicon

        should_create_mikros_department_group >> rail.Label(
            'No') >> create_lastlevel_departmentgroup_in_replicon

        create_lastlevel_departmentgroup_in_replicon >> write_lastlevel_departmentgroup >> rail.Label(
            'Error') >> catch_departmentgroup_level_error

        catch_departmentgroup_level_error >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_departmentgroup_level_child_dag)
