from datetime import datetime as dt
from uuid import uuid4
from airflow.models import Variable
import rail
from wcg.user_import.utils.custom_methods import get_current_supervisor_from_schedule

def create_update_user_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_user_child_dag_id,
        description="WCG User Import - Update Existing User (Workato Steps 24-234)",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_user_child_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="check_if_user_disabled"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="check_if_user_disabled",
            end_task="catch_and_log_errors"
        )

        check_if_user_disabled = rail.IfOperator(
            task_id="check_if_user_disabled",
            test=lambda dag_run: dag_run.conf.get("inactive", "").strip().lower() == "yes",
            yes_task="disable_user_login",
            no_task="get_user_details"
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id="disable_user_login",
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri")
            }
        )

        write_disabled_user_logs = rail.WriteLogOperator(
            task_id="write_disabled_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message="User login disabled",
            severity="Success",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "firstname": dag_run.conf.get("firstname", ""),
                "lastname": dag_run.conf.get("lastname", ""),
                "action": "Disable",
                "status": "Success",
                "details": "User disabled successfully",
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": dag_run.conf.get("user_uri")
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else None
        )

        update_internal_id_oef = rail.RepliconServiceOperator(
            task_id="update_internal_id_oef",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf.get("user_uri"),
                "customFieldUri": dag_run.conf.get("netsuite_internal_id_oef_uri"),
                "value": dag_run.conf.get("employeeid", "")
            }
        )

        check_if_department_changed = rail.IfOperator(
            task_id="check_if_department_changed",
            test=lambda dag_run: (
                dag_run.conf.get("department_uri") and
                dag_run.conf.get("department_uri") != rail.result("get_user_details", {}).get("userDetails", {}).get("department", {}).get("uri")
            ),
            yes_task="update_department_for_user",
            no_task="check_if_employee_type_changed"
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id="update_department_for_user",
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "departmentUri": dag_run.conf.get("department_uri")
            }
        )

        check_if_employee_type_changed = rail.IfOperator(
            task_id="check_if_employee_type_changed",
            test=lambda dag_run: (
                dag_run.conf.get("employee_type_uri") and
                dag_run.conf.get("employee_type_uri") != rail.result("get_user_details", {}).get("employeeType", {}).get("uri")
            ),
            yes_task="update_employee_type_for_user",
            no_task="check_if_middle_name_present"
        )

        update_employee_type_for_user = rail.RepliconServiceOperator(
            task_id="update_employee_type_for_user",
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "employeeTypeUri": dag_run.conf.get("employee_type_uri")
            }
        )

        check_if_middle_name_present = rail.IfOperator(
            task_id="check_if_middle_name_present",
            test='{{ dag_run.conf.get("middlename") is not none and dag_run.conf.middlename != "" }}',
            yes_task="update_middle_name_oef",
            no_task="check_if_first_name_changed"
        )

        update_middle_name_oef = rail.RepliconServiceOperator(
            task_id="update_middle_name_oef",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf.get("user_uri"),
                "customFieldUri": dag_run.conf.get("middle_name_oef_uri"),
                "value": dag_run.conf.get("middlename", "")
            }
        )

        check_if_first_name_changed = rail.IfOperator(
            task_id="check_if_first_name_changed",
            test=lambda dag_run: (
                dag_run.conf.get("firstname") and
                dag_run.conf.get("firstname") != rail.result("get_user_details", {}).get("userDetails", {}).get("firstName")
            ),
            yes_task="update_first_name",
            no_task="check_if_last_name_changed"
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id="update_first_name",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf.get("user_uri"),
                },
                "modifications": {
                    "firstName": {
                        "value": dag_run.conf.get("firstname"),
                    }
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        check_if_last_name_changed = rail.IfOperator(
            task_id="check_if_last_name_changed",
            test=lambda dag_run: (
                dag_run.conf.get("lastname") and
                dag_run.conf.get("lastname") != rail.result("get_user_details", {}).get("userDetails", {}).get("lastName")
            ),
            yes_task="update_last_name",
            no_task="check_if_employee_id_changed"
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id="update_last_name",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf.get("user_uri"),
                },
                "modifications": {
                    "lastName": {
                        "value": dag_run.conf.get("lastname"),
                    }
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        check_if_employee_id_changed = rail.IfOperator(
            task_id="check_if_employee_id_changed",
            test=lambda dag_run: (
                dag_run.conf.get("adp_employee_id") and
                dag_run.conf.get("adp_employee_id") != rail.result("get_user_details", {}).get("userDetails", {}).get("employeeId")
            ),
            yes_task="update_employee_id",
            no_task="check_if_release_date_present"
        )

        update_employee_id = rail.RepliconServiceOperator(
            task_id="update_employee_id",
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "employeeId": dag_run.conf.get("adp_employee_id")
            }
        )

        check_if_release_date_present = rail.IfOperator(
            task_id="check_if_release_date_present",
            test=lambda dag_run: (
                dag_run.conf.get("release_date") and
                rail.parse_date(dag_run.conf.get("release_date"), config.REP_DATE_FORMAT) !=
                rail.result("get_user_details", {}).get("userDetails", {}).get("employmentDateRange", {}).get("endDate")
            ),
            yes_task="update_employment_date_range",
            no_task="check_if_supervisor_present"
        )

        update_employment_date_range = rail.RepliconServiceOperator(
            task_id="update_employment_date_range",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf.get("user_uri"),
                },
                "modifications": {
                    "employmentDateRange": {
                        "value": {
                            "startDate": rail.parse_date(dag_run.conf.get("hire_date"), config.REP_DATE_FORMAT) if dag_run.conf.get("hire_date") else None,
                            "endDate": rail.parse_date(dag_run.conf.get("release_date"), config.REP_DATE_FORMAT) if dag_run.conf.get("release_date") else None,
                        }
                    }
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        check_if_supervisor_present = rail.IfOperator(
            task_id="check_if_supervisor_present",
            test='{{ dag_run.conf.get("supervisorempid") is not none and dag_run.conf.supervisorempid != "" }}',
            yes_task="check_if_user_and_supervisor_same",
            no_task="check_if_location_present"
        )

        check_if_user_and_supervisor_same = rail.IfOperator(
            task_id='check_if_user_and_supervisor_same',
            test='{{ dag_run.conf.employeeid == dag_run.conf.supervisorempid }}',
            yes_task='check_if_location_present',
            no_task='check_if_supervisor_found_in_replicon'
        )

        check_if_supervisor_found_in_replicon = rail.IfOperator(
            task_id="check_if_supervisor_found_in_replicon",
            test='{{ dag_run.conf.get("desired_supervisor_uri") is not none and dag_run.conf.desired_supervisor_uri != "" }}',
            yes_task="check_if_supervisor_changed",
            no_task="log_supervisor_for_later_processing"
        )

        check_if_supervisor_changed = rail.IfOperator(
            task_id="check_if_supervisor_changed",
            test=lambda dag_run: (
                dag_run.conf.get("desired_supervisor_uri") != get_current_supervisor_from_schedule(rail.result("get_user_details"))
            ),
            yes_task="log_supervisor_for_later_processing",
            no_task="check_if_location_present"
        )

        log_supervisor_for_later_processing = rail.WriteLogOperator(
            task_id='log_supervisor_for_later_processing',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor assignment queued for processing after user report refresh",
            severity='Pending',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "firstname": dag_run.conf.get("firstname", ""),
                "lastname": dag_run.conf.get("lastname", ""),
                "useruri": dag_run.conf.get("user_uri"),
                "supervisorempid": dag_run.conf.get("supervisorempid", ""),
                "hire_date": dag_run.conf.get("hire_date", ""),
                "action": "Update",
                "status": "Pending",
                "details": "User updated successfully. Supervisor assignment queued for processing"
            }
        )

        check_if_location_present = rail.IfOperator(
            task_id="check_if_location_present",
            test= lambda dag_run: dag_run.conf.get("location") is not None and dag_run.conf.get("location") != dag_run.conf.get("replicon_location"),
            yes_task="assign_location_to_user",
            no_task="check_if_subsidiary_present"
        )

        assign_location_to_user = rail.RepliconServiceOperator(
            task_id="assign_location_to_user",
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "scheduleEntries": [
                    {
                        "location": {
                            "name": dag_run.conf.get("location")
                        },
                        "effectiveDate": rail.get_replicon_date(dt.now()),
                    }
                ]
            }
        )

        check_if_subsidiary_present = rail.IfOperator(
            task_id="check_if_subsidiary_present",
            test='{{ dag_run.conf.get("subsidiary") is not none and dag_run.conf.subsidiary != "" }}',
            yes_task="update_subsidiary_oef",
            no_task="check_if_labor_cost_present"
        )

        update_subsidiary_oef = rail.RepliconServiceOperator(
            task_id="update_subsidiary_oef",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": dag_run.conf.get("user_uri"),
                },
                "modifications": {
                    "customFields": [
                        {
                            "value": {
                                "customField": {
                                    "uri": dag_run.conf.get("subsidiary_field_uri")
                                },
                                "dropDownOption": {
                                    "name": dag_run.conf.get("subsidiary")
                                }
                            }
                        }
                    ]
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        check_if_labor_cost_present = rail.IfOperator(
            task_id="check_if_labor_cost_present",
            test='{{ dag_run.conf.get("labor_cost") is not none and dag_run.conf.labor_cost != "" }}',
            yes_task="update_hourly_cost",
            no_task="write_updated_user_logs"
        )

        update_hourly_cost = rail.RepliconServiceOperator(
            task_id="update_hourly_cost",
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf.get("user_uri"),
                "hourlyRate": {
                    "amount": str(dag_run.conf.get("labor_cost", "0")),
                    "currencyUri": 'urn:replicon-tenant:'+ rail.get_tenant_slug() + ':currency:1'
                },
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf.get("hire_date"), config.REP_DATE_FORMAT) if dag_run.conf.get("hire_date") else None,
                    "endDate": None,
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        write_updated_user_logs = rail.WriteLogOperator(
            task_id="write_updated_user_logs",
            log='{{ dag_run.conf.log_artifact }}',
            message=lambda dag_run: (
                "User updated partially - Supervisor ID same as Employee ID"
                if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                else "User updated successfully with separate API calls per field"
            ),
            severity=lambda dag_run: (
                "Exception"
                if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                else "Success"
            ),
            properties=lambda dag_run: {
                "employeeid": dag_run.conf["employeeid"],
                "firstname": dag_run.conf.get("firstname", ""),
                "lastname": dag_run.conf.get("lastname", ""),
                "action": "Update",
                "status": (
                    "Exception"
                    if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                    else "Success"
                ),
                "details": (
                    f"Supervisor Employee ID ({dag_run.conf.get('supervisorempid')}) is same as Employee ID ({dag_run.conf.get('employeeid')})"
                    if dag_run.conf.get("employeeid") == dag_run.conf.get("supervisorempid")
                    else "User updated successfully"
                ),
                "runid": dag_run.conf.get("runid", "")
            }
        )

        finish_user_update = rail.EmptyOperator(
            task_id='finish_user_update'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.log_artifact }}',
            message='{{ get_error_message() }}',
            severity="Error",
            trigger_rule="one_failed",
            properties={
                "employeeid": '{{ dag_run.conf.employeeid }}',
                "firstname": '{{ dag_run.conf.get("firstname", "") }}',
                "lastname": '{{ dag_run.conf.get("lastname", "") }}',
                "action": "Update",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "runid": '{{ dag_run.conf.runid }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> check_if_user_disabled

        check_if_user_disabled >> rail.Label("Yes") >> disable_user_login >> write_disabled_user_logs >> finish_user_update
        check_if_user_disabled >> rail.Label("No") >> get_user_details >> update_internal_id_oef >> check_if_department_changed

        check_if_department_changed >> rail.Label("Yes") >> update_department_for_user >> check_if_employee_type_changed
        check_if_department_changed >> rail.Label("No") >> check_if_employee_type_changed

        check_if_employee_type_changed >> rail.Label("Yes") >> update_employee_type_for_user >> check_if_middle_name_present
        check_if_employee_type_changed >> rail.Label("No") >> check_if_middle_name_present

        check_if_middle_name_present >> rail.Label("Yes") >> update_middle_name_oef >> check_if_first_name_changed
        check_if_middle_name_present >> rail.Label("No") >> check_if_first_name_changed

        check_if_first_name_changed >> rail.Label("Yes") >> update_first_name >> check_if_last_name_changed
        check_if_first_name_changed >> rail.Label("No") >> check_if_last_name_changed

        check_if_last_name_changed >> rail.Label("Yes") >> update_last_name >> check_if_employee_id_changed
        check_if_last_name_changed >> rail.Label("No") >> check_if_employee_id_changed

        check_if_employee_id_changed >> rail.Label("Yes") >> update_employee_id >> check_if_release_date_present
        check_if_employee_id_changed >> rail.Label("No") >> check_if_release_date_present

        check_if_release_date_present >> rail.Label("Yes") >> update_employment_date_range >> check_if_supervisor_present
        check_if_release_date_present >> rail.Label("No") >> check_if_supervisor_present

        check_if_supervisor_present >> rail.Label("Yes") >> check_if_user_and_supervisor_same
        check_if_supervisor_present >> rail.Label("No") >> check_if_location_present

        check_if_user_and_supervisor_same >> rail.Label("Yes") >> check_if_location_present
        check_if_user_and_supervisor_same >> rail.Label("No") >> check_if_supervisor_found_in_replicon

        check_if_supervisor_found_in_replicon >> rail.Label("Yes") >> check_if_supervisor_changed
        check_if_supervisor_found_in_replicon >> rail.Label("No") >> log_supervisor_for_later_processing >> check_if_location_present

        check_if_supervisor_changed >> rail.Label("Yes") >> log_supervisor_for_later_processing
        check_if_supervisor_changed >> rail.Label("No") >> check_if_location_present

        check_if_location_present >> rail.Label("Yes") >> assign_location_to_user >> check_if_subsidiary_present
        check_if_location_present >> rail.Label("No") >> check_if_subsidiary_present

        check_if_subsidiary_present >> rail.Label("Yes") >> update_subsidiary_oef >> check_if_labor_cost_present
        check_if_subsidiary_present >> rail.Label("No") >> check_if_labor_cost_present

        check_if_labor_cost_present >> rail.Label("Yes") >> update_hourly_cost >> write_updated_user_logs
        check_if_labor_cost_present >> rail.Label("No") >> write_updated_user_logs

        write_updated_user_logs >> finish_user_update >> catch_and_log_errors

    return dag


rail.for_each_instance(create_update_user_child_dag)
