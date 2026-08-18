# Forked from:
# https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/9450b95/db-cleanup/airflow-db-cleanup.py
"""
A maintenance workflow that periodically cleans out old records from the database. This is done to keep the
database performance from degrading over time.
"""
import logging
from datetime import datetime, timedelta
import dateutil.parser
import rail

from sqlalchemy import func, and_, exists
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

from airflow.configuration import conf
from airflow.models import DAG, DagModel, DagRun, DagTag, Log, XCom, SlaMiss, TaskInstance, Variable
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.jobs.job import Job as BaseJob
from airflow.operators.python import PythonOperator

try:
    from airflow.utils import timezone
    now = timezone.utcnow
except ImportError:
    now = datetime.utcnow

# airflow-db-cleanup
DAG_ID = 'system_airflow_db_cleanup'
START_DATE = datetime(2022, 1, 1)
# How often to Run. @daily - Once a day at Midnight (UTC)
SCHEDULE_INTERVAL = timedelta(days=2)
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "system"
DAG_TAGS = ['system_maintenance']
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that arE 30 days old or older.
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 60
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS_VAR_NAME = "airflow_db_cleanup__max_db_entry_age_in_days"

# Prints the database entries which will be getting deleted; set to False
# to avoid printing large lists and slowdown process
DEFAULT_PRINT_DELETES = "False"
DEFAULT_PRINT_DELETES_VAR_NAME = "airflow_db_cleanup__print_deletes"
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE_VAR_NAME = "airflow_db_cleanup__enable_delete"

# get dag model last schedule run
try:
    dag_model_last_scheduler_run = DagModel.last_scheduler_run
except AttributeError:
    dag_model_last_scheduler_run = DagModel.last_parsed_time

# List of all the objects that will be deleted. Comment out the DB objects you
# want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": BaseJob,
        "age_check_column": BaseJob.latest_heartbeat,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": True,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column": dag_model_last_scheduler_run,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    }]

# Check for TaskReschedule model
try:
    from airflow.models import TaskReschedule
    DATABASE_OBJECTS.append({
        "airflow_db_model": TaskReschedule,
        "age_check_column": TaskReschedule.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for TaskFail model
try:
    from airflow.models import TaskFail
    DATABASE_OBJECTS.append({
        "airflow_db_model": TaskFail,
        "age_check_column": TaskFail.start_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for RenderedTaskInstanceFields model
try:
    from airflow.models import RenderedTaskInstanceFields
    DATABASE_OBJECTS.append({
        "airflow_db_model": RenderedTaskInstanceFields,
        "run_id_column": RenderedTaskInstanceFields.run_id if hasattr(RenderedTaskInstanceFields, "run_id") else None,
        "age_check_column": None if hasattr(RenderedTaskInstanceFields, "run_id") else RenderedTaskInstanceFields.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for ImportError model
try:
    from airflow.models import ImportError as airflow_import_error
    DATABASE_OBJECTS.append({
        "airflow_db_model": airflow_import_error,
        "age_check_column": airflow_import_error.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for celery executor
airflow_executor = str(conf.get("core", "executor"))
logging.info("Airflow Executor: " + str(airflow_executor))
if airflow_executor == "CeleryExecutor":
    logging.info("Including Celery Modules")
    try:
        from celery.backends.database.models import Task, TaskSet  # pylint: disable=import-error
        DATABASE_OBJECTS.extend((
            {
                "airflow_db_model": Task,
                "age_check_column": Task.date_done,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None
            },
            {
                "airflow_db_model": TaskSet,
                "age_check_column": TaskSet.date_done,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None
            }))

    except Exception as e:
        logging.error(e)

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    tags=DAG_TAGS
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False


def enable_delete():
    val = Variable.get(ENABLE_DELETE_VAR_NAME, 'True')
    if val not in ('True', 'False'):
        logging.info(
            f"{ENABLE_DELETE_VAR_NAME} variable is set to unrecognized value '{val}', Using 'False' instead")
        return False
    return val == "True"


def can_print_delete():
    val = Variable.get(DEFAULT_PRINT_DELETES_VAR_NAME, DEFAULT_PRINT_DELETES)
    if val not in ('True', 'False'):
        logging.info(
            f"{DEFAULT_PRINT_DELETES_VAR_NAME} variable is set to unrecognized value '{val}', Using 'False' instead")
        return False
    return val == "True"


def print_configuration_function(**context):
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get(
            "maxDBEntryAgeInDays", None
        )
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(dag_run_conf))
    if (max_db_entry_age_in_days is None or max_db_entry_age_in_days < 1):
        max_db_entry_age_in_days = int(
            Variable.get(
                DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS_VAR_NAME,
                DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS))
        logging.info(
            "maxDBEntryAgeInDays conf variable isn't included or " +
            "value is less than 1. Using Default '" +
            str(max_db_entry_age_in_days) + "'"
        )
    # pylint: disable=invalid-unary-operand-type
    max_date = now() + timedelta(-max_db_entry_age_in_days)
    logging.info("Finished Loading Configurations")
    logging.info("")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(enable_delete()))
    logging.info("")

    logging.info("Setting max_execution_date to XCom for Downstream Processes")
    context["ti"].xcom_push(key="max_date", value=max_date.isoformat())


print_configuration = PythonOperator(
    task_id='print_configuration',
    python_callable=print_configuration_function,
    dag=dag)


# pylint: disable=too-many-statements disable=too-many-branches
@provide_session
def cleanup_function(db_obj, session=NEW_SESSION, **context):

    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(
        task_ids=print_configuration.task_id, key="max_date"
    )
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

    airflow_db_model = db_obj.get("airflow_db_model")
    state = db_obj.get("state")
    age_check_column = db_obj.get("age_check_column")
    keep_last = db_obj.get("keep_last")
    keep_last_filters = db_obj.get("keep_last_filters")
    keep_last_group_by = db_obj.get("keep_last_group_by")
    run_id_column = db_obj.get("run_id_column")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(enable_delete()))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("state:                    " + str(state))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("keep_last:                " + str(keep_last))
    logging.info("keep_last_filters:        " + str(keep_last_filters))
    logging.info("keep_last_group_by:       " + str(keep_last_group_by))
    logging.info("run_id_column:            " + str(run_id_column))

    logging.info("")

    logging.info("Running Cleanup Process...")

    try:
        query = session.query(airflow_db_model).options(
            load_only(age_check_column)
        )

        logging.info("INITIAL QUERY : " + str(query))

        if keep_last:

            subquery = session.query(func.max(DagRun.execution_date))
            # workaround for MySQL "table specified twice" issue
            # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
            if keep_last_filters is not None:
                for entry in keep_last_filters:
                    subquery = subquery.filter(entry)

                logging.info("SUB QUERY [keep_last_filters]: " + str(subquery))

            if keep_last_group_by is not None:
                subquery = subquery.group_by(keep_last_group_by)
                logging.info(
                    "SUB QUERY [keep_last_group_by]: " + str(subquery))

            subquery = subquery.from_self()

            query = query.filter(
                and_(age_check_column.notin_(subquery)),
                and_(age_check_column <= max_date)
            )

        elif run_id_column:
            if age_check_column:
                raise Exception(
                    "Both run_id_column and age_check_column are set. Only one should be set")

            query = query.filter(~exists().where(
                DagRun.run_id == run_id_column))
            logging.info("FILTER [run_id_column]: " + str(query))

        else:
            query = query.filter(age_check_column <= max_date,)

        if can_print_delete():
            entries_to_delete = query.all()

            logging.info("Query: " + str(query))
            logging.info(
                "Process will be Deleting the following " +
                str(airflow_db_model.__name__) + "(s):"
            )
            for entry in entries_to_delete:
                logging.info(
                    "\tEntry: " + str(entry) + ", Date: " +
                    str(entry.__dict__[str(age_check_column).split(".")[1]])
                )

            logging.info(
                "Process will be Deleting " + str(len(entries_to_delete)) + " " +
                str(airflow_db_model.__name__) + "(s)"
            )
        else:
            logging.warning(
                "You've opted to skip printing the db entries to be deleted. Set PRINT_DELETES to True to show entries!!!")

        if enable_delete():
            logging.info("Performing Delete...")
            if airflow_db_model.__name__ == 'DagModel':
                logging.info('Deleting tags...')
                ids_query = query.from_self().with_entities(DagModel.dag_id)
                tags_query = session.query(DagTag).filter(
                    DagTag.dag_id.in_(ids_query))
                logging.info('Tags delete Query: ' + str(tags_query))
                tags_query.delete(synchronize_session=False)
                logging.info('Deleting task instance...')
                ti_query = session.query(TaskInstance).filter(
                    TaskInstance.dag_id.in_(ids_query))
                logging.info('Task Instance delete Query: ' + str(ti_query))
                ti_query.delete(synchronize_session=False)
            # using bulk delete
            query.delete(synchronize_session=False)
            session.commit()
            logging.info("Finished Performing Delete")
        else:
            logging.warning(
                f"You've opted to skip deleting the db entries. Delete the '{ENABLE_DELETE_VAR_NAME}' variable (or set it to True) to delete entries!!!")

        logging.info("Finished Running Cleanup Process")

    except ProgrammingError as program_error:
        logging.error(program_error)
        logging.error(str(airflow_db_model) +
                      " is not present in the metadata. Skipping...")


for db_object in DATABASE_OBJECTS:

    cleanup_op = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        op_args=[db_object],
        dag=dag
    )

    print_configuration.set_downstream(cleanup_op)


@provide_session
def do_cleanup_session(session=NEW_SESSION):
    session.execute(
        f'''DELETE FROM session WHERE expiry < '{rail.result('print_configuration','max_date')}';''')
    session.commit()


cleanup_op = PythonOperator(
    task_id='cleanup_session',
    python_callable=do_cleanup_session,
)
print_configuration.set_downstream(cleanup_op)

cleanup_op = None
