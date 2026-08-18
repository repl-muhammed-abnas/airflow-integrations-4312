region = "us-east-1"
environment = "pre-production"
execution_timeout_days = 14
timezone = "America/New_York"
master_max_active_run = 1

level2_countries = ["United States of America", "Canada"]

dl_time_export_format="DatalakeTimeExtract"
ps_india_report_name="PSIndiaApprovalData"
cp_report_name="CPApprovalData"
paycodes_to_exclude = (
    "NC2",
    "AW2",
    "NC3",
    "AW3",
    "BSH",
    "B1E",
    "B5E",
    "B1N",
    "B5N",
    "NCF",
    "AWF",
    "R1E",
    "R1N",
    "R1W",
    "AWS",
    "NCS",
    "MS2",
    "MS3",
)
