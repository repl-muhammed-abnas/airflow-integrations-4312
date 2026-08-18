import rail
from datetime import timedelta
from rail.operators.procore.version_mapper import ProcoreEquipmentVersion
from ce_procore_integration.equipment_sync.utils.constants import ProcoreEquipmentStatus, Operation


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='Sync Equipment to Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_operation_type',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_operation_type = rail.IfOperator(
            task_id='check_operation_type',
            test=lambda dag_run: dag_run.conf['batch']['operation'] == Operation.CREATE,
            yes_task='create_equipment',
            no_task='bulk_update_equipment'
        )

        def get_create_payload(dag_run):
            equipment = dag_run.conf['batch']['equipments']
            status = ProcoreEquipmentStatus.ACTIVE if equipment[
                'ce_status'] else ProcoreEquipmentStatus.INACTIVE
            status_id = next(
                (s['id'] for s in dag_run.conf['statuses'] if s['name'] == status), '')
            return {
                'identification_number': equipment['ce_code'],
                'name': equipment['ce_name'],
                'status_id': status_id,
                'type_id': dag_run.conf['type_id'],
                'category_id': dag_run.conf['category_id'],
                'ownership': 'OWNED'
            }

        create_equipment = rail.ProcoreApiOperator(
            task_id='create_equipment',
            endpoint='/companies/{{ dag_run.conf.company_id }}/equipment_register',
            method='POST',
            version=ProcoreEquipmentVersion.EQUIPMENT_REGISTER,
            data=get_create_payload
        )

        def get_bulk_update_payload(dag_run):
            equipments = []
            for equipment in dag_run.conf['batch']['equipments']:
                status = ProcoreEquipmentStatus.ACTIVE if equipment[
                    'ce_status'] else ProcoreEquipmentStatus.INACTIVE
                status_id = next(
                    (s['id'] for s in dag_run.conf['statuses'] if s['name'] == status), '')
                equipments.append({
                    'id': equipment['procore_id'],
                    'identification_number': equipment['ce_code'],
                    'name': equipment['ce_name'],
                    'status_id': status_id,
                    'type_id': dag_run.conf['type_id'],
                    'category_id': dag_run.conf['category_id'],
                    'ownership': 'OWNED'
                })
            return equipments
        bulk_update_equipment = rail.ProcoreApiOperator(
            task_id='bulk_update_equipment',
            endpoint='/companies/{{ dag_run.conf.company_id }}/equipment_register/bulk_update',
            method='PATCH',
            version=ProcoreEquipmentVersion.EQUIPMENT_REGISTER,
            data=get_bulk_update_payload
        )

        def get_error_message(dag_run):
            code = None
            name = None
            batch = dag_run.conf.get('batch')
            operation = batch.get('operation', 'unknown')
            err = rail.render_template('{{ get_error_message() }}')

            if operation == Operation.CREATE:
                code = batch.get('equipments', {}).get('ce_code')
                name = batch.get('equipments', {}).get('ce_name')
                return {
                    'operation': operation,
                    'code': code,
                    'name': name,
                    'status': 'Error: Could not create equipment',
                    'reason': err
                }
            elif operation == Operation.UPDATE:
                batches = batch.get('equipments', [])
                code = ','.join(
                    list(map(lambda item: item.get('ce_code'), batches)))
                name = ','.join(
                    list(map(lambda item: item.get('ce_name'), batches)))
                return {
                    'operation': operation,
                    'code': code,
                    'name': name,
                    'status': 'Error: Could not Update equipment',
                    'reason': err
                }
            return {
                'code': code,
                'name': name,
                'operation': operation,
                'status': 'Error',
                'reason': err
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: get_error_message(dag_run)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> check_operation_type

        check_operation_type >> rail.Label(
            'Yes') >> create_equipment >> catch_error
        check_operation_type >> rail.Label(
            'No') >> bulk_update_equipment >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
