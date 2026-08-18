from datetime import timedelta
from airflow.models import Variable
import rail
from rail import get_current_context


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/replicon_datastore/config.py


def create_child_append_timeentries_sub_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_replicon_datastore_append_timeentries_subchild_{config.instance}',
        description=f'Oxfordfinancial Append Time Entries - Sub Child V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.subchild_dag_process_append_time_entries,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timedata'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_timedata',
            end_task='log_dagrun_to_sumo'
        )

        def get_timedata_to_append():
            dag_run_conf = get_current_context()['dag_run'].conf
            clienthouseholdidlength = dag_run_conf['clienthouseholdidlength']
            timeentryid = dag_run_conf['timeentryid']
            firstname = dag_run_conf['firstname']
            lastname = dag_run_conf['lastname']
            middlename = dag_run_conf['middlename']
            userinitials = dag_run_conf['userinitials']
            tasks = dag_run_conf['tasks']
            servicenamebasedonuri = dag_run_conf['servicenamebasedonuri']
            usercurrentdepartment = dag_run_conf['usercurrentdepartment']
            projects = dag_run_conf['projects']
            hoursworked = dag_run_conf['hoursworked']
            entrydate = dag_run_conf['entrydate']
            timesheetperiod = dag_run_conf['timesheetperiod']
            submittedon = dag_run_conf['submittedon']
            loginname = dag_run_conf['loginname']
            comments = dag_run_conf['comments']
            clienthouseholdid = dag_run_conf['clienthouseholdid']
            usersfid = dag_run_conf['usersfid']
            advisorcontactfid = dag_run_conf['advisorcontactfid']
            oxfordcompanyname = dag_run_conf['oxfordcompanyname']
            oxfordcompanyid = dag_run_conf['oxfordcompanyid']
            timesheeturi = dag_run_conf['timesheeturi']
            timeoffhrs = dag_run_conf['timeoffhrs']
            timeofftype = dag_run_conf['timeofftype']
            approvedon = dag_run_conf['approvedon']
            approvedby = dag_run_conf['approvedby']

            # pylint: disable=line-too-long
            if clienthouseholdidlength and clienthouseholdidlength == 18:
                return f"{timeentryid}||{firstname}|{lastname}|{middlename}|{userinitials}|{(tasks.replace('|', '<;>') if tasks else '')}|" + \
                    f"{servicenamebasedonuri}|{usercurrentdepartment}|{projects}|{hoursworked}|{entrydate}|{timesheetperiod}|{submittedon}|" + \
                    f"{submittedon}|{loginname}|{(comments.replace('|', '<;>') if comments else '')}|{clienthouseholdid}|{usersfid}|{advisorcontactfid}|" + \
                    f"{oxfordcompanyname}|{oxfordcompanyid}|{(timesheeturi.split(':')[-1])}|{timeoffhrs}|" + \
                    f"{(timeofftype.replace('|', '<;>') if timeofftype else '')}|{approvedon}|{approvedby}"
            if float(hoursworked) == 0:
                return f"{timeentryid}||{firstname}|{lastname}|{middlename}|{userinitials}|||" + \
                    f"{usercurrentdepartment}|||{entrydate}|{timesheetperiod}|{submittedon}|{submittedon}|" + \
                    f"{loginname}|{(comments.replace('|', '<;>') if comments else '')}||{usersfid}||" + \
                    f"||{(timesheeturi.split(':')[-1])}|{timeoffhrs}|" + \
                    f"{(timeofftype.replace('|', '<;>') if timeofftype else '')}|{approvedon}|{approvedby}"
            if float(hoursworked) != 0:
                householdfirmid = dag_run_conf['householdfirmid']
                return f"{timeentryid}|{householdfirmid}|{firstname}|{lastname}|{middlename}|{userinitials}|{(tasks.replace('|', '<;>') if tasks else '')}|" + \
                    f"{servicenamebasedonuri}|{usercurrentdepartment}|{projects}|{hoursworked}|{entrydate}|{timesheetperiod}|{submittedon}|" + \
                    f"{submittedon}|{loginname}|{(comments.replace('|', '<;>') if comments else '')}|{clienthouseholdid}|{usersfid}||" + \
                    f"{oxfordcompanyname}|{oxfordcompanyid}|{(timesheeturi.split(':')[-1])}|{timeoffhrs}|" + \
                    f"{(timeofftype.replace('|', '<;>') if timeofftype else '')}|{approvedon}|{approvedby}"
            return ''
        get_timedata = rail.PythonOperator(
            task_id='get_timedata',
            python_callable=get_timedata_to_append
        )

        is_timedata_to_append = rail.IfOperator(
            task_id='is_timedata_to_append',
            test="{{ result('get_timedata') | is_truthy }}",
            yes_task='write_timedata_to_lookup',
            no_task='log_dagrun_to_sumo'
        )

        write_timedata_to_lookup = rail.WriteLogOperator(
            task_id='write_timedata_to_lookup',
            log='{{ dag_run.conf.log }}',
            message="Add Timedata to Lookup",
            properties={
                'timedata': "{{ result('get_timedata') }}"
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_timedata >> is_timedata_to_append

        is_timedata_to_append >> rail.Label(
            'Yes') >> write_timedata_to_lookup >> log_dagrun_to_sumo

        is_timedata_to_append >> rail.Label(
            'No') >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_append_timeentries_sub_dag)
