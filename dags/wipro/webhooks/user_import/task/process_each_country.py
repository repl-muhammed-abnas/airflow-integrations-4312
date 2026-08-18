
from datetime import timedelta
from airflow.models import Variable
import rail

task_groups = []

def process_country(groupid,config,cnt,country, trigger_dag_id):
    with rail.TaskGroup(group_id=groupid,prefix_group_id=False):

        can_process_users = rail.IfOperator(
            task_id=f"can_process_{cnt}_users",
            test=lambda:Variable.get(
                f"wiprohris_can_process_{cnt}_records", default_var="true").lower() == 'true',
            yes_task=f"query_all_{cnt}_users"
        )

        query_all_users = rail.QueryCollectionOperator(
            task_id=f"query_all_{cnt}_users",
            query=f"""SELECT * FROM userdeltarecords WHERE country='{country}' """,
            name=f"{cnt}_users"
        )

        if_users = rail.IfOperator(
            task_id=f"if_{cnt}_users",
            test='{{result("query_all_'+cnt+'_users","length") > 0}}',
            yes_task=f"process_user_start_{cnt}"
        )

        process_user_start = rail.EmptyOperator(
            task_id=f"process_user_start_{cnt}")

        process_user = rail.TriggerDagRunOperator(
            task_id=f"process_user_for_{cnt}",
            trigger_dag_id=trigger_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda cnt=cnt:{
                f"{cnt}_users": rail.result(f"query_all_{cnt}_users")
            }
        )


        can_process_users >> rail.Label("Yes") >> \
        query_all_users >> if_users >> rail.Label("Yes") >> process_user_start >> process_user

    return can_process_users