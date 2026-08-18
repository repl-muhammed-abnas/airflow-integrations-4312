import rail
from technicolorg3.user_import.utils.request_payload import get_customfield_dropdown_option_uris, get_udf_query


def process_udfs_task_group(udf, next_udf=None):
    with rail.TaskGroup(group_id=f'process_{udf}_task', prefix_group_id=False):

        current_udf_values = rail.CreateCollectionOperator(
            task_id=f'current_{udf}_values',
            source=lambda: rail.result(f'get_{udf}_dropdown'),
            name=f'{udf}values'
        )

        new_udf_values = rail.QueryCollectionOperator(
            task_id=f'new_{udf}_values',
            query=get_udf_query(udf),
            name=f'{udf}'
        )

        is_udf_new_values = rail.IfOperator(
            task_id=f'is_{udf}_new_values',
            test=lambda: rail.result(f'new_{udf}_values', 'length') > 0,
            yes_task=f'put_dropdown_options_{udf}',
            no_task=f'current_{next_udf}_values' if next_udf else 'catch_error'
        )

        put_dropdown_options_udf = rail.RepliconServiceOperator(
            task_id=f'put_dropdown_options_{udf}',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda: {
                'customFieldUri': rail.result('get_required_user_customfields')[f'{udf}_uri'],
                'customFieldDropDownOptionUris': get_customfield_dropdown_option_uris(
                    udf, f'get_{udf}_dropdown', rail.result(f'new_{udf}_values'))
            }
        )

        current_udf_values >> new_udf_values >> \
            is_udf_new_values >> rail.Label(
                'Yes') >> put_dropdown_options_udf

    return (current_udf_values, is_udf_new_values, put_dropdown_options_udf)
