import rail
null = None

def get_conf_for_process_ack_payload(dag_run):
    return {
        **dict(dag_run.conf),
        **{
            "sender": "psa",
            "erp": "psa"
		}
    }

def get_create_download_batch(export_uri, fileformat_uri):
    return {
		"columnUris": [],
		"sort": [],
		"filterExpression": {
			"leftExpression": {
				"leftExpression": null,
				"operatorUri": null,
				"rightExpression": null,
				"value": null,
				"filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
			},
			"operatorUri": "urn:replicon:filter-operator:in",
			"rightExpression": {
				"leftExpression": null,
				"operatorUri": null,
				"rightExpression": null,
				"value": {
					"uri": null,
					"uris": [export_uri],
					"bool": null,
					"date": null,
					"money": null,
					"number": null,
					"text": null,
					"time": null,
					"calendarDayDurationValue": null,
					"workdayDurationValue": null,
					"dateRange": null,
					"dateTimeUtc": null,
					"dateTimeUtcRange": null
				},
				"filterDefinitionUri": null
			},
			"value": null,
			"filterDefinitionUri": null
		},
		"fileFormatScriptUri": fileformat_uri
	}

def get_psa_org_child_hierarchy_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:code"
        ],
        "parentUri": rail.result("get_psa_org_unit")
    }

def get_psa_cost_center_child_hierarchy_payload():
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:code"
        ],
        "parentUri": rail.result("get_psa_cost_center")
    }

def get_update_oef_acknowlegement_payload(dag_run):
    return {
        "objectUri": dag_run.conf['timeexporturi'],
        "value": {
            "definition": {
            	"uri": dag_run.conf['oefuri']
            },
            "textValue": "Yes"
        }
    }
