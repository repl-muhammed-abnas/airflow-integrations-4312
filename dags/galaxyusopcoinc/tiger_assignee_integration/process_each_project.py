import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.macros import get_error_message
from galaxyusopcoinc.tiger_assignee_integration.utils import request_payload


def create_child_dag_wbs(config):
    project_dags = []

    for idx in range(0, config.BATCH_SIZE_PROJECT):
        with rail.create_airflow_dag(
            dag_id=f'vialtopartners_tiger_assignee_integration_child_process_each_project_{config.instance}' \
                if idx ==0 else f'vialtopartners_tiger_assignee_integration_child_process_each_project_{config.instance}_batch_{idx}',
            description='Vialto Partners Tiger Assignee Integration Process Each Project',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_each_project,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            def create_batch_of_list(data, len_of_data, batch_size):
                for index in range(0, len_of_data, batch_size):
                    yield data[index: index+batch_size]

            def create_add_remove_merge(dag_run):
                tags_to_add = rail.load_all_records(dag_run.conf['tagstoadd'])
                tags_to_remove = rail.load_all_records(dag_run.conf['tagstoremove'])
                add_splited_in_batches = list(create_batch_of_list(data = tags_to_add, len_of_data=len(tags_to_add), batch_size=config.BATCH_SIZE))
                remove_splited_in_batches =list(create_batch_of_list(data = tags_to_remove, len_of_data=len(tags_to_remove), batch_size=config.BATCH_SIZE))
                len_add_splited_in_batches = len(add_splited_in_batches)
                len_remove_splited_in_batches = len(remove_splited_in_batches)
                final_result =[]
                for idx in range(0, min(len_add_splited_in_batches, len_remove_splited_in_batches)):
                    final_result.append({"add":add_splited_in_batches[idx], "remove":remove_splited_in_batches[idx]})

                if len_add_splited_in_batches != 0 or len_remove_splited_in_batches !=0:
                    if len_add_splited_in_batches == len_remove_splited_in_batches:
                        return final_result

                    if len_add_splited_in_batches > len_remove_splited_in_batches:
                        for idx in range(len_remove_splited_in_batches, len_add_splited_in_batches):
                            final_result.append({"add":add_splited_in_batches[idx], "remove":[]})
                    else:
                        for idx in range(len_add_splited_in_batches, len_remove_splited_in_batches):
                            final_result.append({"add":[], "remove":remove_splited_in_batches[idx]})

                return final_result

            apply_assigneeids_modification = rail.RepliconServiceCallForEachItemOperator(
                task_id='apply_assigneeids_modification',
                items=create_add_remove_merge,
                endpoint='services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags',
                data=request_payload.apply_assigneeids_modification2
            )

            # log_project_succesfull_completion = rail.WriteLogOperator(
            #     task_id='log_project_succesfull_completion',
            #     log= "{{ dag_run.conf.successlogs }}",
            #     items="{{dag_run.conf.assigneedetails}}",
            #     message=lambda item: 'Assignee Added Successfully' if item[
            #         'status'] == 'ACTIVE' else 'Assignee Removed Successfully',
            #     severity='Success',
            #     properties=lambda dag_run, item: {
            #         "projectname": dag_run.conf['projectname'],
            #         "clientname": dag_run.conf['clientname'],
            #         "clientshortname": item['clientshortname'],
            #         'assigneeid': item['assigneeid'],
            #         'assigneestatus': item['status'],
            #         'details':'Assignee Added Successfully' if item[
            #         'status'] == 'ACTIVE' else 'Assignee Removed Successfully',
            #         'status': 'Success',
            #         "jobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            #     }
            # )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.errorlogs }}',
                trigger_rule='one_failed',
                severity='Error',
                items="{{dag_run.conf.assigneedetails}}",
                message='{{ get_error_message() }}',
                properties=lambda dag_run, item: {
                    "projectname": dag_run.conf['projectname'],
                    "clientname": dag_run.conf['clientname'],
                    "clientshortname": item['clientshortname'],
                    'assigneeid': item['assigneeid'],
                    'assigneestatus': item['status'],
                    'details':get_error_message(rail.get_current_context()),
                    'status': 'Error',
                    "jobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
                },
            )

            apply_assigneeids_modification >> catch_and_log_errors

        project_dags.append(dag)

    return dag


rail.for_each_instance(create_child_dag_wbs)
