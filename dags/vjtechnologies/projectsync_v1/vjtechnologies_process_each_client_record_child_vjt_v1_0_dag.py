
from datetime import timedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_each_client_record_child_dagid,
        description=f'VJTechnologies_{config.entity_name}_Process_each_client_record_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_client_code_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_client_code_present',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_client_code_present=rail.IfOperator(
            task_id='if_client_code_present',
            test='''{{ dag_run.conf.code | is_truthy }}''',
            yes_task="search_client",
            no_task="catch_and_log_error",
        )

        def get_required_client(response,dag_run):
            required_client = {}
            for client in response['rows']:
                if client['cells'][1]['textValue'] == dag_run.conf['code']:
                    required_client = client
                    break
            return required_client

        search_client=rail.RepliconServiceOperator(
            task_id='search_client',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "1000",
              "columnUris": [
                "urn:replicon:client-list-column:name",
                "urn:replicon:client-list-column:code"
              ],
              "sort": [],
              "filterExpression": {
                "leftExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": null,
                  "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                  "leftExpression": null,
                  "operatorUri": null,
                  "rightExpression": null,
                  "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": "{{ dag_run.conf.code }}",
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                  },
                  "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
              }
            },
            data_handler=get_required_client
        )

        if_client_not_found=rail.IfOperator(
            task_id='if_client_not_found',
            test=lambda: not bool(rail.result('search_client')),
            yes_task="create_client",
            no_task="update_client",
        )

        create_client=rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data={
              "target": null,
              "modifications": {
                "nameToApply": {
                  "value": "{{ dag_run.conf.name }}"
                },
                "codeToApply": {
                  "value": "{{ dag_run.conf.code }}"
                },
                "descriptionToApply": null,
                "statusToApply": "true",
                "clientContactToApply": null,
                "clientAddressToApply": null,
                "billingAddressToApply": null,
                "billingRatesToApply": {
                  "billingRates": [
                    {
                      "billingRate": {
                        "uri": "urn:replicon:project-specific-billing-rate",
                        "name": null
                      },
                      "rateSchedule": null
                    },
                    {
                      "billingRate": {
                        "uri": "urn:replicon:user-specific-billing-rate",
                        "name": null
                      },
                      "rateSchedule": null
                    }
                  ]
                },
                "clientManagerToApply": null,
                "clientSharingToApply": null,
                "expenseCodesToApply": null,
                "customFieldsToApply": []
              },
              "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
              "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        add_log_client_created=rail.WriteLogOperator(
            task_id='add_log_client_created',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties={
              'jobid': "{{dag_run.conf.callerjobid}}",
              "type": "client",
              "client": "{{ dag_run.conf.name }}",
              "project": null,
              "code": "{{ dag_run.conf.code }}",
              "task": null,
              "status": "Success",
              "reason": "Client Created",
              "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        update_client=rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data={
              "target": {
                "uri": null,
                "name": null,
                "code": "{{ dag_run.conf.code }}",
                "parameterCorrelationId": null
              },
              "modifications": {
                "nameToApply": {
                  "value": "{{ dag_run.conf.name }}"
                },
                "codeToApply": null,
                "descriptionToApply": null,
                "statusToApply": "true",
                "clientContactToApply": null,
                "clientAddressToApply": null,
                "billingAddressToApply": null,
                "billingRatesToApply": {
                  "billingRates": [
                    {
                      "billingRate": {
                        "uri": "urn:replicon:project-specific-billing-rate",
                        "name": null
                      },
                      "rateSchedule": null
                    },
                    {
                      "billingRate": {
                        "uri": "urn:replicon:user-specific-billing-rate",
                        "name": null
                      },
                      "rateSchedule": null
                    }
                  ]
                },
                "clientManagerToApply": null,
                "clientSharingToApply": null,
                "expenseCodesToApply": null,
                "customFieldsToApply": []
              },
              "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
              "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        add_log_client_updated=rail.WriteLogOperator(
            task_id='add_log_client_updated',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties={
              'jobid': "{{dag_run.conf.callerjobid}}",
              "type": "client",
              "client": "{{ dag_run.conf.name }}",
              "project": null,
              "code": "{{ dag_run.conf.code }}",
              "task": null,
              "status": "Success",
              "reason": "Client Updated",
              "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
              'jobid': "{{dag_run.conf.callerjobid}}",
              "type": "client",
              "client": "{{ dag_run.conf.name }}",
              "project": null,
              "code": "{{ dag_run.conf.code }}",
              "task": null,
              "status": "Error",
              "reason": "{{get_error_message()}}",
              "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_client_code_present
        if_client_code_present
        if_client_code_present >> rail.Label('Yes')  >> search_client >> if_client_not_found
        if_client_not_found >> rail.Label('Yes')  >> create_client >> add_log_client_created >> catch_and_log_error
        if_client_not_found >> rail.Label('No') >> update_client >> add_log_client_updated >> catch_and_log_error
        if_client_code_present >> rail.Label('No') >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
