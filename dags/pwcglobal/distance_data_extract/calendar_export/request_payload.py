import rail

def get_run_report_payload(dag_run):
    return {
        "reportParameters": [
            {
                "reportUri":  dag_run.conf['report_uri'],
                "filterValues": [
                    {
                        "reportFilterUri": dag_run.conf['report_filter_uri'],
                        "value": None
                    },
                    {
                        "reportFilterUri": dag_run.conf['report_filter_uri'],
                        "value": dag_run.conf['start_date']
                    },
                    {
                        "reportFilterUri": dag_run.conf['report_filter_uri'],
                        "value": dag_run.conf['end_date']

                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)
