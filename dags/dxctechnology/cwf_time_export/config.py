region = 'us-east-2'
environment = 'pre-production'

dag_max_active_runs = 128
dag_max_active_tasks = 128
field_glass_report_name = 'CWFTime - Fieldglass'

input_date_format = '%d %B %Y'  # date format in 3 April 2021
output_date_format = '%m/%d/%Y'
entry_date_format = '%d/%m/%Y'
report_date_format = '%d %B %Y '
# pylint: disable=line-too-long
error_template = '{{ get_error_message() }}'
disabled = True
