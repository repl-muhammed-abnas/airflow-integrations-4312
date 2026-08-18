from pendulum import datetime
import rail

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_homepage_message_update_master_{config.instance}',
        description=f'Capgemini Homepage Message Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 11, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        def get_homepage_messages(response):
            return list(map(lambda data: {
                "targetUri": data["uri"],
                "message": data["message"],
                "url": data["url"] if data["url"] else null,
                "categoryUri": data["category"]["uri"],
                "enabled": data["enabled"],
                "startDate": data["startDate"],
                "endDate": data["endDate"],
                "groups": {
                    "locationUri": data["location"]["uri"] if data["location"] else null,
                    "divisionUri": null,
                    "costCenterUri": data["costCenter"]["uri"] if data["costCenter"] else null,
                    "serviceCenterUri": null,
                    "departmentGroupUri": data["departmentGroup"]["uri"] if data["departmentGroup"] else null,
                    "employeeTypeGroupUri": data["employeeTypeGroup"]["uri"] if data["employeeTypeGroup"] else null
                }
            }, response))

        get_page_of_homepage_messages = rail.RepliconServiceOperator(
            task_id='get_page_of_homepage_messages',
            endpoint='/services/OverviewPageMessageService1.svc/GetPageOfOverviewPageMessages',
            data={
                "page": "1",
                "pageSize": "100",
                "includeDisabled": "false"
            },
            data_handler=get_homepage_messages
        )

        if_messsages_present = rail.IfOperator(
            task_id='if_messsages_present',
            test=lambda: len(rail.result("get_page_of_homepage_messages")) > 0,
            yes_task='trigger_put_homepage_message'
        )

        trigger_put_homepage_message = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_put_homepage_message',
            items='{{ result("get_page_of_homepage_messages") | to_json }}',
            trigger_dag_id=f'capgemini_homepage_message_update_process_messages_child_{config.instance}',
            conf=lambda item: {
                "message_payload": item
            }
        )

        get_page_of_homepage_messages >> if_messsages_present >> rail.Label("Yes") >> trigger_put_homepage_message

    return dag

rail.for_each_instance(create_dag)
