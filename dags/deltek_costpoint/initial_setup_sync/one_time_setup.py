from datetime import timedelta
import uuid
from airflow.models import Variable
import rail
# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_onetime_setup_main_{config.instance}',
        description=f'deltek_costpoint_onetime_setup_main_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='foreach_oef_text_flow'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='foreach_oef_text_flow',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_oef_text_info():
            return [config.proj_user_company]

        foreach_oef_text_flow = rail.ForEachOperator(
            task_id='foreach_oef_text_flow',
            items=get_oef_text_info,
            start_task='create_replicon_text_oef',
            end_task='foreach_oef_text_flow_end'
        )

        create_replicon_text_oef = rail.RepliconServiceOperator(
            task_id='create_replicon_text_oef',
            endpoint='services/ObjectExtensionTextDefinitionService1.svc/PutObjectExtensionTextDefinition',
            data=lambda: {
                "objectExtensionTextDefinition": {
                    "target": {
                        "name": rail.result('foreach_oef_text_flow'),
                        "uri": null
                    },
                    "name": rail.result('foreach_oef_text_flow'),
                    "description": rail.result('foreach_oef_text_flow')
                }
            }
        )

        binding_oef_text_to_user = rail.RepliconServiceOperator(
            task_id='binding_oef_text_to_user',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('create_replicon_text_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:user"
            }
        )

        foreach_oef_text_flow_end = rail.EmptyOperator(
            task_id='foreach_oef_text_flow_end',
        )

        def get_oef_tag_info():
            return [config.glc,
                    config.paytype,
                    config.taxableentity,
                    config.empclass,
                    config.flsaexempt,
                    config.plc]

        foreach_oef_tag_flow = rail.ForEachOperator(
            task_id='foreach_oef_tag_flow',
            items=get_oef_tag_info,
            start_task='create_replicon_oef',
            end_task='foreach_oef_tag_flow_end'
        )

        create_replicon_oef = rail.RepliconServiceOperator(
            task_id='create_replicon_oef',
            endpoint='services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTagDefinition',
            data=lambda: {
                "objectExtensionTagDefinition": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_oef_tag_flow')
                    },
                    "name": rail.result('foreach_oef_tag_flow'),
                    "code": rail.result('foreach_oef_tag_flow'),
                    "description": rail.result('foreach_oef_tag_flow'),
                    "tags": []
                }
            }
        )

        binding_oef_to_user = rail.RepliconServiceOperator(
            task_id='binding_oef_to_user',
            endpoint='services/ObjectExtensionService1.svc/BindObjectExtensionField',
            data=lambda: {
                "objectExtensionDefinitionUri": rail.result('create_replicon_oef')['uri'],
                "bindingContextUri": "urn:replicon:object-type:user"
            }
        )

        foreach_oef_tag_flow_end = rail.EmptyOperator(
            task_id='foreach_oef_tag_flow_end',
        )

        get_customfield_groups = rail.RepliconServiceOperator(
            task_id='get_customfield_groups',
            endpoint='services/CustomFieldService1.svc/GetCustomFieldGroups'
        )

        def get_udf_text_info():
            return [config.proj_purchase_order_no,
                    config.proj_opportunity_id,
                    config.proj_project_classification,
                    config.proj_user_company]

        foreach_udf_text_flow = rail.ForEachOperator(
            task_id='foreach_udf_text_flow',
            items=get_udf_text_info,
            start_task='create_replicon_udf',
            end_task='foreach_udf_text_flow_end'
        )

        create_replicon_udf = rail.RepliconServiceOperator(
            task_id='create_replicon_udf',
            endpoint='services/CustomFieldService1.svc/PutCustomField',
            data=lambda: {
                "customField": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_udf_text_flow'),
                        "groupUri": rail.find_first_by_attr_and_get_attr(rail.result('get_customfield_groups'), 'displayText', 'Project', 'uri')
                    },
                    "name": rail.result('foreach_udf_text_flow'),
                    "customFieldGroupUri": rail.find_first_by_attr_and_get_attr(rail.result('get_customfield_groups'), 'displayText', 'Project', 'uri'),
                    "customFieldTypeUri": "urn:replicon:custom-field-type:text",
                    "isRequired": "false",
                    "isVisible": "true",
                    "isEnabled": "true",
                    "textConfiguration": {}
                }
            }
        )

        foreach_udf_text_flow_end = rail.EmptyOperator(
            task_id='foreach_udf_text_flow_end',
        )

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs: {
                'generallabourcategories': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.glc, 'uri'),
                'paytype': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.paytype, 'uri'),
                'oeftaxableentity': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.taxableentity, 'uri'),
                'oefemployeeclass': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.empclass, 'uri'),
                'oefflsaexempt': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.flsaexempt, 'uri'),
                'projectlaborcategory': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.plc, 'uri'),
            },
        )

        declare_list_1 = rail.SetVariableOperator(
            task_id='declare_list_1',
            append=False,
            name='flsaexempt_tags',
            value=[]
        )

        foreach_flsaexempt_oef_flow = rail.ForEachOperator(
            task_id='foreach_flsaexempt_oef_flow',
            items=config.flsaexempt_tags,
            start_task='create_flsaexempt_new_draft',
            end_task='foreach_flsaexempt_oef_flow_end'
        )

        create_flsaexempt_new_draft = rail.RepliconServiceOperator(
            task_id="create_flsaexempt_new_draft",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_user_oefs')['oefflsaexempt']
            },
        )

        update_flsaexempt_oef_name = rail.RepliconServiceOperator(
            task_id="update_flsaexempt_oef_name",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_flsaexempt_new_draft'),
                "name": rail.result('foreach_flsaexempt_oef_flow')['name']
            },
        )

        update_flsaexempt_oef_code = rail.RepliconServiceOperator(
            task_id="update_flsaexempt_oef_code",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_flsaexempt_new_draft'),
                "code": rail.result('foreach_flsaexempt_oef_flow')['code']
            },
        )

        publish_flsaexempt_draft = rail.RepliconServiceOperator(
            task_id="publish_flsaexempt_draft",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_flsaexempt_new_draft')
            },
        )

        insert_to_list_1 = rail.SetVariableOperator(
            task_id='insert_to_list_1',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value=lambda: {
                "code": rail.result('foreach_flsaexempt_oef_flow')['code'],
                "name": rail.result('publish_flsaexempt_draft')['displayText'],
                "uri": rail.result('publish_flsaexempt_draft')['uri']
            }
        )

        foreach_flsaexempt_oef_flow_end = rail.EmptyOperator(
            task_id='foreach_flsaexempt_oef_flow_end',
        )

        def get_oeftag_request():
            objectExtensionTags = []
            new_flsaexempt_tags = rail.get_dag_run_var(
                rail.result('declare_list_1')['name'])
            for tag in new_flsaexempt_tags:
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": tag['name'],
                    "code": tag['code'],
                    "description": null,
                    "isEnabled": "true"
                })

            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_user_oefs')['oefflsaexempt']
                },
                "objectExtensionTags": objectExtensionTags
            }

        add_new_flsaexempt_oef_tags = rail.RepliconServiceOperator(
            task_id="add_new_flsaexempt_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_oeftag_request,
        )

        def get_groups_request():
            groups_history = []
            for group in config.employee_type_options:
                groups_history.append(
                    {
                        "target": null,
                        "parameterCorrelationId": null,
                        "modificationToApply": {
                            "name": group['name'],
                            "codeToApply": {
                                "value": group['code']
                            },
                            "descriptionToApply": {
                                "value": group['name']
                            },
                            "isEnabled": "1"
                        }
                    }
                )
            return groups_history

        create_employeetype_or_applymodifications = rail.RepliconServiceOperator(
            task_id='create_employeetype_or_applymodifications',
            endpoint='services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupHierarchyOrApplyModifications',
            data=lambda: {
                "hierarchy": get_groups_request(),
                "modificationOptionUri": null,
                "unitOfWorkId": "" + str(uuid.uuid4())
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error:' +
            rail.render_template("{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> foreach_oef_text_flow >> create_replicon_text_oef >> binding_oef_text_to_user >> foreach_oef_text_flow_end
        foreach_oef_text_flow >> foreach_oef_text_flow_end >> \
            foreach_oef_tag_flow >> create_replicon_oef >> binding_oef_to_user >> foreach_oef_tag_flow_end
        foreach_oef_tag_flow >> foreach_oef_tag_flow_end >> get_customfield_groups >> \
            foreach_udf_text_flow >> create_replicon_udf >> foreach_udf_text_flow_end
        foreach_udf_text_flow >> foreach_udf_text_flow_end >> get_user_oefs >> declare_list_1 >> foreach_flsaexempt_oef_flow >> \
            create_flsaexempt_new_draft >> update_flsaexempt_oef_name >> update_flsaexempt_oef_code >> \
            publish_flsaexempt_draft >> insert_to_list_1 >> foreach_flsaexempt_oef_flow_end
        foreach_flsaexempt_oef_flow >> foreach_flsaexempt_oef_flow_end >> \
            add_new_flsaexempt_oef_tags >> create_employeetype_or_applymodifications >> \
            catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
