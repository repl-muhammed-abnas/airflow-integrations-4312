from datetime import timedelta
import itertools
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_departmentupdate_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_department_update_child_{config.instance}',
        description=f'Live| Mccarthy Child_department update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_department_group_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_department_group_details',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_department_groups(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return list(map(lambda item: {
                'departmentname': item['cells'][0]['textValue'],
                'departmenturi': item['cells'][0]['uri'],
                'fullpath': rail.smartjoin_by_delim(
                    [x['textValue'] for x in item['cells'][1]['cellCollection']], '|') if [
                        x['textValue'] for x in item['cells'][1]['cellCollection']] else '',
                'length': len([x['textValue'] for x in item['cells'][1]['cellCollection']]) if [
                    x['textValue'] for x in item['cells'][1]['cellCollection']] else 0
            }, flatten_rows)) if flatten_rows else []
        get_department_group_details = rail.RepliconServicePageOperator(
            task_id='get_department_group_details',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 1000000,
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ]
            },
            page_handler=page_handler,
            all_result_data_handler=get_department_groups
        )

        create_replicon_departmentgroup_data = rail.CreateCollectionOperator(
            task_id='create_replicon_departmentgroup_data',
            source=lambda: rail.result('get_department_group_details'),
            name="departmentgroupdata"
        )

        def get_rows(item):
            department_to_assign = item['Department'].split('|')
            department = f"McCarthy Holdings, Inc.|{rail.smartjoin_by_delim(department_to_assign, '|')}"
            department_length = len(department.split('|'))
            return [department, department_length]
        create_feedfile_group_csv = rail.WriteCSVFileOperator(
            task_id='create_feedfile_group_csv',
            source="{{ dag_run.conf.groupcollection }}",
            header=['department', 'length'],
            row=get_rows
        )

        create_feedfile_group_collection = rail.CreateCollectionOperator(
            task_id='create_feedfile_group_collection',
            source="{{ result('create_feedfile_group_csv') }}",
            name="departmentrawdata"
        )

        query_departments_to_add = rail.QueryCollectionOperator(
            task_id='query_departments_to_add',
            query="""SELECT DISTINCT department FROM departmentrawdata WHERE
                    LOWER(department) NOT IN (SELECT DISTINCT LOWER(fullpath)
                    FROM departmentgroupdata) AND (NULLIF(department, '') IS NOT NULL) AND length < 7"""
        )

        is_departments_to_add = rail.IfOperator(
            task_id='is_departments_to_add',
            test="{{ result('query_departments_to_add', 'length') > 0 }}",
            yes_task="trigger_departmentgroup_add",
            no_task="dagrun_log_to_sumo"
        )

        trigger_departmentgroup_add = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_departmentgroup_add',
            retries=0,
            items="{{ result('query_departments_to_add') }}",
            trigger_dag_id=f'mccarthy_user_import_child_department_add_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "parentdepartmenturi": rail.find_first_by_attr_and_get_attr(
                    rail.result(
                        'get_department_group_details'), 'fullpath', 'McCarthy Holdings, Inc.',
                    'departmenturi', ''),
                "department": item['department']
            }
        )

        wait_for_departmentgroup_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_departmentgroup_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_departmentgroup_add') }}"
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_department_group_details >> create_replicon_departmentgroup_data >> create_feedfile_group_csv >> \
            create_feedfile_group_collection >> query_departments_to_add >> is_departments_to_add
        is_departments_to_add >> rail.Label(
            'Yes') >> trigger_departmentgroup_add >> wait_for_departmentgroup_add >> dagrun_log_to_sumo
        is_departments_to_add >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_departmentupdate_dag)
