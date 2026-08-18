import rail
from assuredpartnersinc.timeoff_balance_export_v1.utils import custom_methods


def get_timeoff_data(code, collection_code, timeoff_data_columns):
    with rail.TaskGroup(group_id=f'{collection_code}_timeoff_data', prefix_group_id=False):

        timeoff_data_columns = custom_methods.get_timeoff_data_columns(
            timeoff_data_columns)

        query_code_data = rail.QueryCollectionOperator(
            task_id=f'query_{code}_code_data',
            query=custom_methods.get_timeoff_code_data_query(code)
        )

        timeoff_code_data = rail.CreateCollectionOperator(
            task_id=f'{code}_code_data',
            source='{{ result("query_'+code+'_code_data") }}',
            columns={
                "employeeid": "employeeid1",
                "companycode": "companycode1",
                "timeofftype": "timeofftype",
                "timeoffaccrued": "timeoffaccrued",
                "timeofftaken": "timeofftaken",
                "timeoffbalance": "timeoffbalance",
                "headercode": "headercode",
                "ptocode": "ptocode"
            },
            name=f'{code}_code_data'
        )

        query_to_merge_data = rail.QueryCollectionOperator(
            task_id=f'query_to_merge_{collection_code}_data',
            query=custom_methods.get_merging_query(collection_code)
        )

        timeoff_data = rail.CreateCollectionOperator(
            task_id=f'{collection_code}_data',
            source='{{ result("query_to_merge_'+collection_code+'_data") }}',
            columns=timeoff_data_columns,
            name=f'{collection_code}_code_data'
        )

        query_code_data >> timeoff_code_data >> query_to_merge_data >> timeoff_data

        return query_code_data, timeoff_data
