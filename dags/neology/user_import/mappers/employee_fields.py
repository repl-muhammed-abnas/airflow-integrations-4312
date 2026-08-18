required_employee_fields_list = [
	{
		"bamboohr_name": "Employee #",
		"replicon_name": "Employee ID",
		"parent_name": "Personal",
		"mandatory": True,
		"type": "text",
		"field_attr": "employeenumber",
		"action": "add"
	},
	{
		"bamboohr_name": "First Name",
		"replicon_name": "First Name",
		"parent_name": "Personal",
		"mandatory": True,
		"type": "text",
		"field_attr": "firstname",
		"action": "add"
	},
	{
		"bamboohr_name": "Last Name",
		"replicon_name": "Last Name",
		"parent_name": "Personal",
		"mandatory": True,
		"type": "text",
		"field_attr": "lastname",
		"action": "add"
	},
	{
		"bamboohr_name": "Name",
		"replicon_name": "Display Name",
		"parent_name": "Calculated",
		"mandatory": True,
		"type": "text",
		"field_attr": "preferredname",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Work Email",
		"replicon_name": "Email",
		"parent_name": "Personal",
		"mandatory": True,
		"type": "text",
		"field_attr": "workemail",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Hire Date",
		"replicon_name": "Start Date",
		"parent_name": "Job",
		"mandatory": True,
		"type": "date",
		"field_attr": "hiredate",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Termination Date",
		"replicon_name": "End Date",
		"parent_name": "Calculated",
		"mandatory": False,
		"type": "date",
		"field_attr": "terminationdate",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Original Hire Date",
		"replicon_name": "Original Hire Date",
		"parent_name": "Job",
		"mandatory": False,
		"type": "oef",
		"field_attr": "originalhiredate_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Status",
		"replicon_name": "Status",
		"parent_name": "Default Status",
		"mandatory": True,
		"type": "text",
		"field_attr": "status",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Is Supervisor",
		"replicon_name": "Is Supervisor",
		"parent_name": "Calculated",
		"mandatory": False,
		"type": "boolean",
		"field_attr": "issupervisor",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Supervisor ID",
		"replicon_name": "Supervisor",
		"parent_name": "Calculated",
		"mandatory": True,
		"type": "text",
		"field_attr": "supervisorid",
		"action": "add_update"
	},
	{
		"bamboohr_name": "State",
		"replicon_name": "State",
		"parent_name": "Personal",
		"mandatory": False,
		"type": "oef",
		"field_attr": "state_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Intl Payroll Number",
		"replicon_name": "Payroll Number",
		"parent_name": "Custom",
		"mandatory": True,
		"type": "oef",
		"field_attr": "payrollnumber_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Pay Status",
		"replicon_name": "Pay Status",
		"parent_name": "Custom",
		"mandatory": True,
		"type": "oef",
		"field_attr": "paystatus_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Pay Country",
		"replicon_name": "Pay Country",
		"parent_name": "Custom",
		"mandatory": True,
		"type": "oef",
		"field_attr": "paycountry_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "External ID",
		"replicon_name": "AssociateOID",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "oef",
		"field_attr": "associateOID_oef",
		"action": "add"
	},
	{
		"bamboohr_name": "Rate Code",
		"replicon_name": "Rate Code",
		"parent_name": "Custom",
		"mandatory": True,
		"type": "oef",
		"field_attr": "ratecode_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "PTO Job Level",
		"replicon_name": "Job Level",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "oef",
		"field_attr": "joblevel_oef",
		"action": "add"
	},
	{
		"bamboohr_name": "Department",
		"replicon_name": "Department",
		"parent_name": "Job Information",
		"mandatory": False,
		"type": "text",
		"field_attr": "department",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Division",
		"replicon_name": "Subsidiary",
		"parent_name": "Job Information",
		"mandatory": False,
		"type": "text",
		"field_attr": "subsidiary",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Location",
		"replicon_name": "Location",
		"parent_name": "Job Information",
		"mandatory": False,
		"type": "text",
		"field_attr": "location",
		"action": "add_update"
	},
	{
		"bamboohr_name": "US Payroll Code",
		"replicon_name": "ADP Company Code",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "oef",
		"oef_type": "list",
		"field_attr": "adpcompanycode_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Country",
		"replicon_name": "Country",
		"parent_name": "Personal",
		"mandatory": False,
		"type": "text",
		"field_attr": "country",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Contract Agency",
		"replicon_name": "Agency",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "oef",
		"oef_type": "list",
		"field_attr": "agency_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Job Title",
		"replicon_name": "Job Title",
		"parent_name": "Job Information",
		"mandatory": True,
		"type": "oef",
		"field_attr": "jobtitle_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Reports To",
		"replicon_name": "Reports To",
		"parent_name": "Job Information",
		"mandatory": False,
		"type": "text",
		"field_attr": "reportsto",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Effective Date",
		"replicon_name": "Job Information Effective Date",
		"parent_name": "Job Information",
		"mandatory": False,
		"type": "date",
		"field_attr": "jobinfoeffectivedate",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Employment Status",
		"replicon_name": "Employee Type",
		"parent_name": "Employment Status",
		"mandatory": False,
		"type": "oef",
		"oef_type": "list",
		"field_attr": "employeetype_oef",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Effective Date",
		"replicon_name": "Employee Status Effective Date",
		"parent_name": "Employment Status",
		"mandatory": False,
		"type": "date",
		"field_attr": "empstatuseffectivedate",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Timesheet Type",
		"replicon_name": "Timesheet Template",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "text",
		"field_attr": "timesheettype",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Time Zone",
		"replicon_name": "Time Zone",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "text",
		"field_attr": "timezone",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Holiday Calendar",
		"replicon_name": "Holiday Calendar",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "text",
		"field_attr": "holidaycalendar",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Pay Rule",
		"replicon_name": "Pay Rule",
		"parent_name": "Custom",
		"mandatory": False,
		"type": "text",
		"field_attr": "payrule",
		"action": "add_update"
	},
	{
		"bamboohr_name": "Last changed",
		"replicon_name": "Last Changed",
		"parent_name": "Calculated",
		"mandatory": False,
		"type": "date",
		"field_attr": "lastchanged",
		"action": ""
	},
	{
		"bamboohr_name": "BambooHR Integration",
		"replicon_name": "BambooHR Integration",
		"parent_name": "",
		"mandatory": False,
		"type": "oef",
		"field_attr": "bamboohr_integration_oef",
		"action": "add"
	}
]
