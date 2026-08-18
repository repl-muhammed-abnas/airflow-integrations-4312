from tsystems.time_import import config

timesheet_templates = {
  "Internal only Duration": config.timesheet_dist,
  "Internal without controls": config.timesheet_dist,
  "External Employee": config.timesheet_dist,
  "Shift workers only Duration": config.timesheet_dist,
  "Shift workers": config.timesheet_dist,
  "Internal Std": config.timesheet_inout_dist,
  "HR200 integr. RZ": config.timesheet_inout_dist_with_oef,
  "HR200 FZ=AZ": config.timesheet_inout_dist_with_oef,
  "Internal Worktype": config.timesheet_inout_dist_with_oef,
  "HR200 tariffrei": config.timesheet_inout_dist_with_oef,
  "HR200 Tarif": config.timesheet_inout_dist_with_oef,
}