import uuid
from nttdata.shift_automation.utils import python_callable_methods
from nttdata.shift_automation.utils import request_payload
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'nttdata_default_shift_assignment_per_user_child_{config.instance}',
        description=f'NTTData Default shift assignment per user child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_shift_schedule_summary_foruser=rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary_foruser',
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=request_payload.get_shift_schedule_summary_payload
        )

        create_assigned_shift_dates_collection = rail.CreateCollectionOperator(
            task_id='create_assigned_shift_dates_collection',
            source=python_callable_methods.get_assigned_shift_list,
            name='assigned_shift_dates_collection',
            columns=["date", "week", "shift"]
        )

        create_dates_to_consider_collection = rail.CreateCollectionOperator(
            task_id='create_dates_to_consider_collection',
            source=python_callable_methods.get_dates_to_consider_list,
            name='dates_to_consider_collection',
            columns=["seq", "date", "day", "dateday", "datemonth", "dateyear", "week"]
        )

        is_country_pan = rail.IfOperator(
            task_id='is_country_pan',
            test='{{ dag_run.conf.country | matches(["PAN"]) }}',
            yes_task='working_days_pan_list',
            no_task='working_days_other_country_list'
        )

        working_days_pan_list = rail.QueryCollectionOperator(
            task_id='working_days_pan_list',
            query="SELECT * FROM dates_to_consider_collection WHERE (day=0 OR day=1 OR \
                day=2 OR day=3 OR day=4 OR day=5) AND week NOT IN (SELECT DISTINCT week FROM assigned_shift_dates_collection)"
        )

        is_working_days_pan_list_exists = rail.IfOperator(
            task_id='is_working_days_pan_list_exists',
            test='{{ result("working_days_pan_list", "length") > 0 }}',
            yes_task='create_shift_assignment_list'
        )

        working_days_other_country_list = rail.QueryCollectionOperator(
            task_id='working_days_other_country_list',
            query="SELECT * FROM dates_to_consider_collection WHERE (day=0 OR day=1 OR \
                day=2 OR day=3 OR day=4) AND week NOT IN (SELECT DISTINCT week FROM assigned_shift_dates_collection)"
        )

        is_working_days_other_country_list_exists = rail.IfOperator(
            task_id='is_working_days_other_country_list_exists',
            test='{{ result("working_days_other_country_list", "length") > 0 }}',
            yes_task='create_shift_assignment_list',
        )

        create_shift_assignment_list = rail.PythonOperator(
            task_id='create_shift_assignment_list',
            python_callable=python_callable_methods.get_shift_assignment_list
        )

        bulk_put_shift_assignments = rail.RepliconServiceOperator(
            task_id='bulk_put_shift_assignments',
            endpoint='/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments',
            data=lambda: {
                "assignments": rail.result("create_shift_assignment_list"),
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        get_shift_schedule_summary_foruser >> create_assigned_shift_dates_collection >> create_dates_to_consider_collection >> is_country_pan
        is_country_pan >> rail.Label("Yes") >> working_days_pan_list >> is_working_days_pan_list_exists
        is_working_days_pan_list_exists >> rail.Label("Yes") >> create_shift_assignment_list
        is_country_pan >> rail.Label("No") >> working_days_other_country_list >> is_working_days_other_country_list_exists
        is_working_days_other_country_list_exists >> rail.Label("Yes") >> create_shift_assignment_list
        create_shift_assignment_list >> bulk_put_shift_assignments

    return dag

rail.for_each_instance(create_dag)
