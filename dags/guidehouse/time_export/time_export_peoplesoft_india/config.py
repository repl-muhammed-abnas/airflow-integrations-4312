region = "us-east-1"
environment = "pre-production"
execution_timeout_days = 14
timezone = "America/New_York"

ps_file_prefix = "PPSTime"
india_file_prefix = "INDTime"

file_extension = ".csv.pgp"
export_file_format_name = "PeopleSoftTimeExport"

master_max_active_run = 1

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
