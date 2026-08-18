import rail
from adtalem.user_import.utils.python_callable_method import get_mapper_paygroup_jobcode


def process_mappers_task_group(process_userdag_caller=None):
    with rail.TaskGroup(group_id='process_mappers_task', prefix_group_id=False):

        mapper_paygroup_jobcode = rail.PythonOperator(
            task_id='mapper_paygroup_jobcode',
            python_callable=get_mapper_paygroup_jobcode,
            op_args=[process_userdag_caller]
        )

        search_paygroup_in_mapper = rail.PythonOperator(
            task_id='search_paygroup_in_mapper',
            python_callable=lambda: [
                x for x in rail.result('mapper_paygroup_jobcode')['paygroup_jobcode_mapper'] if x[
                    'pay_group'] == rail.result('mapper_paygroup_jobcode')['paygroup']]
        )

        search_jobcode_in_mapper = rail.PythonOperator(
            task_id='search_jobcode_in_mapper',
            python_callable=lambda: [
                x for x in rail.result('mapper_paygroup_jobcode')['paygroup_jobcode_mapper'] if x[
                    'job_code'] == rail.result('mapper_paygroup_jobcode')['jobcode']]
        )

        mapper_paygroup_jobcode >> search_paygroup_in_mapper >> search_jobcode_in_mapper

        return (mapper_paygroup_jobcode, search_jobcode_in_mapper)
