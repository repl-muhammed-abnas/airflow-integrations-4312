import rail
from technicolorg3.user_import.utils.python_callable_method import get_usermapper_entries


def process_mappers_task_group(user_master_mapper):
    with rail.TaskGroup(group_id='process_mappers_task', prefix_group_id=False):

        is_businessunitname_servicelinename = rail.IfOperator(
            task_id='is_businessunitname_servicelinename',
            test="{{ dag_run.conf.businessunitname == 'The Focus' and dag_run.conf.servicelinename == 'Management & Central Functions' }}",
            yes_task='get_mapper_entries_from_businessunitname',
            no_task='get_mapper_entries_from_country_location'
        )

        get_mapper_entries_from_businessunitname = rail.PythonOperator(
            task_id='get_mapper_entries_from_businessunitname',
            python_callable=get_usermapper_entries,
            op_args=[user_master_mapper, None, None,
                     '{{ dag_run.conf.businessunitname }}']
        )

        get_mapper_entries_from_country_location = rail.PythonOperator(
            task_id='get_mapper_entries_from_country_location',
            python_callable=get_usermapper_entries,
            op_args=[user_master_mapper,
                     '{{ dag_run.conf.country }}', '{{ dag_run.conf.worklocation }}', None]
        )

        get_mapper_entries_from_country = rail.PythonOperator(
            task_id='get_mapper_entries_from_country',
            python_callable=get_usermapper_entries,
            op_args=[user_master_mapper,
                     '{{ dag_run.conf.country }}', None, None]
        )

        get_default_mapper_entries_from_country = rail.PythonOperator(
            task_id='get_default_mapper_entries_from_country',
            python_callable=get_usermapper_entries,
            op_args=[user_master_mapper, 'Default', None, None]
        )

        is_businessunitname_servicelinename >> rail.Label(
            'Yes') >> get_mapper_entries_from_businessunitname >> get_mapper_entries_from_country_location

        is_businessunitname_servicelinename >> rail.Label(
            'No') >> get_mapper_entries_from_country_location

        get_mapper_entries_from_country_location >> get_mapper_entries_from_country >> \
            get_default_mapper_entries_from_country

        return is_businessunitname_servicelinename, get_default_mapper_entries_from_country
