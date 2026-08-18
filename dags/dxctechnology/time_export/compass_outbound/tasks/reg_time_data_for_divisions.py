import rail
from dxctechnology.time_export.compass_outbound.utils import custom_methods

#pylint: disable=too-many-arguments


def get_reg_time_data_for_divisions(code, task_type):
    with rail.TaskGroup(group_id=f'reg_time_data_for_{task_type}', prefix_group_id=False) as create_time_dataset:

        if_dataset_has_data = rail.IfOperator(
            task_id=f'if_{task_type}_has_data',
            test='{{ result("query_'+ task_type + '_data", "length") > 0 }}',
            yes_task=f'{task_type}_final_data',
            no_task=f'finish_{task_type}_dataset'
        )

        final_data = rail.CreateCollectionOperator(
            task_id=f'{task_type}_final_data',
            source=lambda: custom_methods.get_each_company_final_reg_data(rail.result(f"query_{task_type}_data"),
                str(rail.result(f"final_export_{task_type}_data", key="length")), task_type),
            columns=["shortid", "externalsystemidentifier", "perner", "date", "projectname",
                "attendanceabsencetype", "hours", "comments", "iwoexternalsystem", "attribute1", "attribute2",
                "externalprojecttask", "remainingwork", "cfield1", "cfield2", "cfield3", "workorder",
                "ratetype", "tmrole", "gsapbillingkey", "gsaptask", "gsapbillableflag", "bdopportunityid"],
            name=f'{task_type}_final_data'
        )

        log_data_existence_var = rail.SetVariableOperator(
            task_id=f'log_{task_type}_data_var',
            name='data_existence',
            value=[
                {
                    "name": code,
                    "type": "Data",
                    "count": "{{ result('" + task_type + "_final_data', 'length') }}",
                }
            ],
            append=True
        )

        finish_dataset = rail.EmptyOperator(
            task_id=f'finish_{task_type}_dataset'
        )

        if_dataset_has_data >> rail.Label("Yes") >> final_data >> log_data_existence_var >> finish_dataset
        if_dataset_has_data >> rail.Label("No") >> finish_dataset

    return create_time_dataset
