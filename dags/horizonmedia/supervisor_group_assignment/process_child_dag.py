
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_supervisororg_group_process_child_{config.instance}',
        description=f'HorizonMedia_supervisororg_group_process_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_assigned_permission_sets_for_user2_permissionsassignedtouser_15 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_permissionsassignedtouser_15',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}"
            }
        )

        if_wherepolicyuriurnrepliconpolicysupervision_presence_present_16 = rail.IfOperator(
            task_id='if_wherepolicyuriurnrepliconpolicysupervision_presence_present_16',
            test='''{{ result('get_assigned_permission_sets_for_user2_permissionsassignedtouser_15') | find_first_by_attr_and_get_attr('policyUri',"urn:replicon:policy:supervision")| is_truthy }}''',
            yes_task="declare_list_17",
            no_task="finish",
        )

        declare_list_17 = rail.SetVariableOperator(
            task_id='declare_list_17',
            append=False,
            name='finalpolicyscope',
            value=[]
        )

        find_supervisor_reportees_18 = rail.QueryCollectionOperator(
            task_id='find_supervisor_reportees_18',
            query='''SELECT * FROM repliconbasereport'''
        )

        load_all_supervisor_reportees_18 = rail.PythonOperator(
            task_id='load_all_supervisor_reportees_18',
            python_callable=lambda: rail.load_all_records(
                rail.result('find_supervisor_reportees_18'))
        )

        def get_all_supervisor_reportees(supervisor_uri):
            users = list(map(lambda x: {
                'useruri': x['useruri'], 'supervisororggroup': x['supervisororggroup']
            }, filter(
                lambda x: x['supervisoruri'] == supervisor_uri, rail.result('load_all_supervisor_reportees_18'))))
            if users:
                for user in users:
                    users = users + \
                        get_all_supervisor_reportees(user['useruri'])
            return users

        get_supervisor_reportees_18 = rail.PythonOperator(
            task_id='get_supervisor_reportees_18',
            python_callable=lambda: get_all_supervisor_reportees(
                rail.get_dag_run_conf()['supervisoruri'])
        )

        invoke_custom_python_code_19 = rail.PythonOperator(
            task_id='invoke_custom_python_code_19',
            python_callable=lambda: list(map(lambda x: {
                "supervisororggroup": x,
            }, set(map(lambda x: x['supervisororggroup'], rail.result('get_supervisor_reportees_18')))))
        )

        invoke_custom_python_code_20 = rail.PythonOperator(
            task_id='invoke_custom_python_code_20',
            python_callable=lambda: list(
                filter(lambda x: x['uri'],
                       map(lambda x:  {
                           "name": x['supervisororggroup'],
                           "uri": rail.find_first_by_attr_and_get_attr(rail.get_dag_run_conf()['department_groups'], 'displayText', x['supervisororggroup'], 'uri')
                       }, rail.result('invoke_custom_python_code_19'))))
        )

        insert_to_list_21 = rail.SetVariableOperator(
            task_id='insert_to_list_21',
            append=False,
            name='{{ result("declare_list_17").name }}',
            value=lambda: list(map(lambda x: {
                    "departmentGroup": {
                        "uri": x['uri'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                "groupSpecificationModeUri": null,
                "groupDescendantModeUri": null
            }, rail.result('invoke_custom_python_code_20'))) if rail.result('invoke_custom_python_code_20') else []
        )

        if_declare_list_17_list_items_greater_than_0_22 = rail.IfOperator(
            task_id='if_declare_list_17_list_items_greater_than_0_22',
            test='''{{  result('invoke_custom_python_code_20') | is_truthy and result('insert_to_list_21').value | length > 0 }}''',
            yes_task="trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23",
            no_task="horizonmedia_supervisororg_logs_add_entry_25",
        )

        trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23',
            retries=0,
            items=[1],
            trigger_dag_id=f'horizonmedia_supervisororg_group_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "log": rail.result('create_log'),
                "supervisorname": rail.get_dag_run_conf()['supervisorname'],
                "supervisoruri": rail.get_dag_run_conf()['supervisoruri'],
                "policyaccessdata": rail.result('insert_to_list_21')['value']
            }
        )

        wait_for_completion_trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23") }}'
        )

        horizonmedia_supervisororg_logs_add_entry_25 = rail.WriteLogOperator(
            task_id='horizonmedia_supervisororg_logs_add_entry_25',
            log="{{ result('create_log') }}",
            message="na",
            severity="Exception",
            properties={
                "supervisorname": "{{ dag_run.conf.supervisorname }}",
                "status": "Exception",
                "details": "Supervisor Permission is not assigned"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_log >> get_assigned_permission_sets_for_user2_permissionsassignedtouser_15 >> if_wherepolicyuriurnrepliconpolicysupervision_presence_present_16
        if_wherepolicyuriurnrepliconpolicysupervision_presence_present_16 >> rail.Label(
            'yes') >> declare_list_17 >> find_supervisor_reportees_18 >> load_all_supervisor_reportees_18 >> get_supervisor_reportees_18 >> invoke_custom_python_code_19 >> invoke_custom_python_code_20 >> insert_to_list_21 >> if_declare_list_17_list_items_greater_than_0_22
        if_declare_list_17_list_items_greater_than_0_22 >> rail.Label(
            'yes') >> trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23 >> wait_for_completion_trigger_dag_run_live_horizonmedia_supervisororg_group_assignment_childasync_23 >> finish
        if_declare_list_17_list_items_greater_than_0_22 >> rail.Label(
            'no') >> horizonmedia_supervisororg_logs_add_entry_25 >> finish
        if_wherepolicyuriurnrepliconpolicysupervision_presence_present_16 >> rail.Label(
            'no') >> finish

    return dag


rail.for_each_instance(create_dag)
