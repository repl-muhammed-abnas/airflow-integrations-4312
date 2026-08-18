import json
from datetime import timedelta
from pendulum import datetime as dt
import rail

from report_comparison.employee_data.utils.custom_methods import *

null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"maconomy_workbook employee report comparison {config.instance}",
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        catchup=False,
        tags=["maconomy_workbook"],
    ) as dag:

        workato_employee_department_api = rail.SimpleHttpOperator(
            task_id="workato_employee_department_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_employee_department_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_employee_department_mapper = rail.PythonOperator(
            task_id="get_employee_department_mapper",
            python_callable=process_employee_department_data
        )

        workato_applicationaccessrole_api = rail.SimpleHttpOperator(
            task_id="workato_applicationaccessrole_api",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_applicationaccessrole_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_applicationaccessrole_mapper = rail.PythonOperator(
            task_id="get_applicationaccessrole_mapper",
            python_callable=process_applicationaccessrole_data
        )

        workato_business_unit = rail.SimpleHttpOperator(
            task_id="workato_business_unit",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_business_unit_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_business_mapper = rail.PythonOperator(
            task_id="get_business_mapper",
            python_callable=process_business_unit_data
        )

        workato_position = rail.SimpleHttpOperator(
            task_id="workato_position",
            http_conn_id="workato_endpoint",
            endpoint=config.workato_position_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workato_token_var
                + " }}",
            },
        )

        get_position_mapper = rail.PythonOperator(
            task_id="get_position_mapper",
            python_callable=process_position_data
        )

        workbook_logout = rail.SimpleHttpOperator(
            task_id="workbook_logout",
            http_conn_id="workbook_http_connid",
            endpoint="api/auth/logout",
            method="GET"
        )

        workbook_data_api = rail.SimpleHttpOperator(
            task_id="workbook_data_api",
            method="POST",
            http_conn_id="workbook_http_connid",
            endpoint=config.workbook_api,
            headers={
                "Accept-Encoding": "gzip",
                "Connection": "keep-alive",
                "X-HTTP-Method-Override": "GET",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": "Bearer {{ var.value."
                + config.workbook_token_var
                + " }}",
            },
            data=json.dumps({"DataboardId": 10050, "Parameters": {"1": "1"}}),
            response_filter=lambda response: json.loads(response.text),
        )

        workbook_data_python = rail.PythonOperator(
            task_id="workbook_data_python",
            python_callable=process_workbook_data
        )

        maconomy_data = rail.SimpleHttpOperator(
            task_id="maconomy_data",
            method="POST",
            http_conn_id="maconomy_http_connid",
            endpoint=config.maconomy_api,
            headers={
                "Maconomy-Authentication": "X-Reconnect",
                "Accept": "application/vnd.deltek.maconomy.containers+json; version=5.0",
                "Content-Type": "application/vnd.deltek.maconomy.containers+json; version=5.0",
            },
            data=json.dumps(
                {
                    "fields": "thekey,employeenumber,name1,name2,name3,zipcode,postaldistrict,name4,name5,telephone,electronicmailaddress,cnrnumber,country,blocked,companynumber,companyname,salesemployee,accountmanager,position,profession,education,dateemployed,dateendemployment,departmentnumber,employmenttermsnumber,employeetype,primaryemployeecategorynumber,personalemailaddress,personaltitle,maidenname,nameinlocalalphabet,previousemployeenumber,eeoclassification,timesheetstartdate,timesheetenddate,willingtorelocate,alternativejobtitle,dtmpersonnelrequisition,firstname,middlename,lastname,formalfirstname,formalmiddlename,formallastname,overheadcostratepercentage,overheadcostrateaspercentage,employeeoverheadmaintenance,dimensioncombnumber,accountnumber,localaccountnumber,locationname,entityname,projectname,purposename,specification1name,specification2name,specification3name,specification4name,specification5name,specification6name,specification7name,specification8name,specification9name,specification10name,localspec1name,localspec2name,localspec3name,localspec4name,localspec5name,localspec6name,localspec7name,localspec8name,localspec9name,localspec10name,overwriteaccount,overwritelocalaccount,overwritelocation,overwriteentity,overwriteproject,overwritepurpose,overwritespec1,overwritespec2,overwritespec3,overwritespec4,overwritespec5,overwritespec6,overwritespec7,overwritespec8,overwritespec9,overwritespec10,overwritelocalspec1,overwritelocalspec2,overwritelocalspec3,overwritelocalspec4,overwritelocalspec5,overwritelocalspec6,overwritelocalspec7,overwritelocalspec8,overwritelocalspec9,overwritelocalspec10,superioremployee,secretaryemployee,vendornumber,commissionaccount,commissionsetoffaccount,transferlocation,transferentity,transferproject,transferpurpose,transferspec1,transferspec2,transferspec3,transferspec4,transferspec5,transferspec6,transferspec7,transferspec8,transferspec9,transferspec10,transferlocalspec1,transferlocalspec2,transferlocalspec3,transferlocalspec4,transferlocalspec5,transferlocalspec6,transferlocalspec7,transferlocalspec8,transferlocalspec9,transferlocalspec10,costprice,intercompanyprice,billingprice,standardbillingprice,basesalaryrate,overheadcostrate,itemnumber,jobpricegroupnumber,weekcalendarnumber,maxworkingtimeperday,minimumworkingtime,allowedvariance,fixedworkingtimemonday,fixedworkingtimetuesday,fixedworkingtimewednesday,fixedworkingtimethursday,fixedworkingtimefriday,fixedworkingtimesaturday,fixedworkingtimesunday,reductionpercentage,documentarchivenumber,remark1,remark2,remark3,remark4,remark5,basicsalarycode,basicsalary,salarysupplementcode,salarysupplement,overtimeratecode,overtimerate,bank,registrationnumber,bankaccountnumber,pensionscheme,pensionschemeown,pensiontypecompany,pensiontypeown,pensionamountcompany,pensionamountown,incometaxrate,taxallowancepermonth,taxallowance14days,taxallowanceperweek,taxallowanceperday,taxallowancecard,taxallowanceyear,present,employeepopup1,employeepopup2,employeepopup3,employeepopup4,employeepopup5,statistic1,statistic2,statistic3,statistic4,createdby,createddate,changedby,changeddate,versionnumber,accesslevelname,electronicmailwhenapprovalresponsible,electronicmailwhenallocationresponsible,purchaseordernumber,absenceapprover,fixedabsencemonday,fixedabsencetuesday,fixedabsencewednesday,fixedabsencethursday,fixedabsencefriday,fixedabsencesaturday,fixedabsencesunday,workhoursregistration,allowedvarianceworkhours,timecheckedinmondayexpected,timecheckedintuesdayexpected,timecheckedinwednesdayexpected,timecheckedinthursdayexpected,timecheckedinfridayexpected,timecheckedinsaturdayexpected,timecheckedinsundayexpected,timecheckedoutmondayexpected,timecheckedouttuesdayexpected,timecheckedoutwednesdayexpected,timecheckedoutthursdayexpected,timecheckedoutfridayexpected,timecheckedoutsaturdayexpected,timecheckedoutsundayexpected,tutoremployee,mustusetimesheets,subcontractorvendornumber,transfertopeopleplanner,vacationcalendarnumber,substitute1,substitute2,substitute3,substitute4,substitute5,contactpersonnumber,initials,gender,dateofbirth,mobilephone,mobilephone2,telephone2,noticedate,pensiondate,optionlistnumber1,selectedoption1,optionlistnumber2,selectedoption2,optionlistnumber3,selectedoption3,optionlistnumber4,selectedoption4,optionlistnumber5,selectedoption5,optionlistnumber6,selectedoption6,optionlistnumber7,selectedoption7,optionlistnumber8,selectedoption8,optionlistnumber9,selectedoption9,optionlistnumber10,selectedoption10,text1,text2,text3,text4,text5,text6,text7,text8,text9,text10,date1,date2,date3,date4,date5,amount1,amount2,amount3,amount4,amount5,amount6,amount7,amount8,amount9,amount10,real1,real2,real3,real4,real5,boolean1,boolean2,boolean3,boolean4,boolean5,templateemployeenumber,numberofapprovals,numberofactiveapprovals,numberofinactiveapprovals,numberofdueapprovals,numberofdueinactiveapprovals,numberofsubstitutetasks,numberofactivesubstitutetasks,numberofinactivesubstitutetasks,numberofduesubstitutetasks,numberofdueinactivesubstitutetasks,firstdayofabsence,lastdayofabsence,calculatedabsencetype,submitted,submittedby,datesubmitted,timesubmitted,approved,closed,closedby,closingdate,closingtime,numbertobeapproved,numberreadyforapproval,numberapproved,approveraccessinstancekey,instancekey,linkingrulename,timeregistrationunit,compensationmodelname,hrsmartuserid,hrsmartpositioncode,hrsmartsynctimestamp,genericresource,absencehoursperday,permanentlyblocked,checkin,privatephone,lastapprovallistnumber,dimcombversionnumber,transactiontimestamp,approvalgroupinstancekey,usefixedworkingtimeasmaximum,lengthofservicedate,excludeovertime",
                    "restriction": "approved=true",
                    "limit": 0,
                }
            ),
            response_filter=lambda response: json.loads(response.text)
        )

        maconomy_employee_data = rail.PythonOperator(
            task_id="maconomy_employee_data",
            python_callable=process_maconomy_employee_data
        )

        comparison_report = rail.PythonOperator(
            task_id="comparison_report",
            python_callable=comparison_details
        )

        generate_csv_report = rail.PythonOperator(
            task_id="generate_csv_report",
            python_callable=generate_test_report
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("generate_csv_report")}}',
            output_file_name="Employee_Comparison_report_{{ecid()|replace(':','_')}}.csv",
            expires_in_seconds=24 * 7 * 60 * 60,
        )

        send_report_complete_mail = rail.EmailOperator(
            task_id="send_report_complete_mail",
            to=config.tenant_email,
            subject="Employee Report Comparison is completed {{current_time_in_specified_tz()}}",
            html_content="templates/send_completion_mail.html",
        )

        # Task dependencies
        (
            workato_employee_department_api
            >> get_employee_department_mapper
            >> workbook_logout
        )
        (
            workato_applicationaccessrole_api
            >> get_applicationaccessrole_mapper
            >> workbook_logout
        )
        workato_business_unit >> get_business_mapper >> workbook_logout
        workato_position >> get_position_mapper >> workbook_logout
        workbook_logout >> workbook_data_api >> workbook_data_python
        workbook_data_python >> maconomy_data >> maconomy_employee_data
        maconomy_employee_data >> comparison_report
        comparison_report >> generate_csv_report
        generate_csv_report >> generate_download_link >> send_report_complete_mail

        return dag


rail.for_each_instance(create_airflow_dag)
