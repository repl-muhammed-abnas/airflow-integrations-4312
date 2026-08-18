import json
import datetime

def process_paid_invoice_details(invoice_details_raw, invoice_uri, invoice_number_text, paid_date):
    invoice_details = json.loads(invoice_details_raw)
    paid_date_list = paid_date.split("/")
    paid_date_month, paid_date_day, paid_date_year = str(int(paid_date_list[0])), str(int(paid_date_list[1])), str(int(paid_date_list[2]))
    invoice_uri = '"'+invoice_uri+'"'
    invoice_number_text = '"'+invoice_number_text+'"'
    invoice_currency_uri = '"'+invoice_details['invoiceCurrency']['uri']+'"'
    client_uri = '"'+invoice_details['client']['uri']+'"'
    current_datetime = datetime.datetime.now()
    unit_of_work_id = '"'+str(current_datetime.timestamp())+"-"+str(current_datetime)+'"'

    notes_for_customer_text = "null"
    internal_notes_text = "null"
    po_number_text = "null"
    invoice_date_year = "null"
    invoice_date_month = "null"
    invoice_date_day = "null"
    paymentterms_num = "null"
    payment_due_year = "null"
    payment_due_month = "null"
    payment_due_day = "null"
    billing_address_text = "null"
    description_text = "null"
    invoice_template_text = "null"
    notes_for_customer_text = "null"

    for k in invoice_details['customMetadata']:
        if k["keyUri"] == "urn:replicon:invoice-metadata-key:internal-notes":
            internal_notes_text = '"'+k['value']['text']+'"' if "text" in k["value"] else "null"
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:po-number":
            po_number_text = '"'+k['value']['text']+'"' if "text" in k["value"] else "null"
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:invoice-date":
            invoice_date_year = str(k["value"]["date"]["year"])
            invoice_date_month = str(k["value"]["date"]["month"])
            invoice_date_day = str(k["value"]["date"]["day"])
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:payment-terms":
            paymentterms_num = str(k['value']['number']) if "number" in k["value"] else "null"
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:payment-due-date":
            payment_due_year = str(k["value"]["date"]["year"])
            payment_due_month = str(k["value"]["date"]["month"])
            payment_due_day = str(k["value"]["date"]["day"])
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:billing-address":
            billing_address_text = '"'+k['value']['text']+'"' if "text" in k["value"] else "null"
            billing_address_text = billing_address_text.replace('\n','\\n')
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:description":
            description_text = '"'+k['value']['text']+'"' if "text" in k["value"] else "null"
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:invoice-template":
            invoice_template_text = '"'+k['value']['uri']+'"' if "uri" in k["value"] else "null"
        elif k["keyUri"] == "urn:replicon:invoice-metadata-key:notes-for-customer":
            notes_for_customer_text = '"'+k['value']['text']+'"' if "text" in k["value"] else "null"
    return """{
		"invoice": {
			"target": {
			"uri": """+invoice_uri+""",
			"invoiceNumberText": null,
			"parameterCorrelationId": null
			},
			"client": {
			"uri": """+client_uri+""",
			"name": null,
			"parameterCorrelationId": null
			},
			"invoiceNumberText": """+invoice_number_text+""",
			"invoiceCurrency": {
			"uri": """+invoice_currency_uri+""",
			"name": null,
			"symbol": null
			},
			"customMetadata": [
			{
				"keyUri": "urn:replicon:invoice-metadata-key:internal-notes",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": """+internal_notes_text+""",
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:po-number",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": """+po_number_text+""",
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:invoice-date",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": {
					"year": """+invoice_date_year+""",
					"month": """+invoice_date_month+""",
					"day": """+invoice_date_day+"""
				},
				"number": null,
				"text": null,
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:payment-terms",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": """+paymentterms_num+""",
				"text": null,
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:payment-due-date",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": {
					"year": """+payment_due_year+""",
					"month": """+payment_due_month+""",
					"day": """+payment_due_day+"""
				},
				"number": null,
				"text": null,
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:billing-address",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": """+billing_address_text+""",
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:description",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": """+description_text+""",
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:summarize-columns",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": null,
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": [{"uri":null,"slug":null,"bool":null,"date":null,"number":null,"text":"project","time":null,"calendarDayDurationValue":null,"workdayDurationValue":null,"dateRange":null},\
					{"uri":null,"slug":null,"bool":null,"date":null,"number":null,"text":"user","time":null,"calendarDayDurationValue":null,"workdayDurationValue":null,"dateRange":null},\
					{"uri":null,"slug":null,"bool":null,"date":null,"number":null,"text":"billingRate","time":null,"calendarDayDurationValue":null,"workdayDurationValue":null,"dateRange":null},\
					{"uri":null,"slug":null,"bool":null,"date":null,"number":null,"text":"entryDate","time":null,"calendarDayDurationValue":null,"workdayDurationValue":null,"dateRange":null},\
					{"uri":null,"slug":null,"bool":null,"date":null,"number":null,"text":"task","time":null,"calendarDayDurationValue":null,"workdayDurationValue":null,"dateRange":null}]
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:invoice-template",
				"value": {
				"uri": """+invoice_template_text+""",
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": null,
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:paid-date",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": {
					"year": """+paid_date_year+""",
					"month": """+paid_date_month+""",
					"day": """+paid_date_day+"""
				},
				"number": null,
				"text": null,
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			},
			{
				"keyUri": "urn:replicon:invoice-metadata-key:notes-for-customer",
				"value": {
				"uri": null,
				"slug": null,
				"bool": null,
				"date": null,
				"number": null,
				"text": """+notes_for_customer_text+""",
				"time": null,
				"calendarDayDurationValue": null,
				"workdayDurationValue": null,
				"dateRange": null,
				"collection": []
				}
			}
			],
			"extensionFieldValues": []
		},
		"unitOfWorkId": """+unit_of_work_id+"""
		}"""
