# Sheet: Leave Assignment Updated
# Rows: 20, Columns: 7
null = None
TIMEOFF_MAPPER = [
  # Spec update: PHL - Annual Leave / PHL - Sick Leave must apply to hires
  # ON or AFTER 1-Jan-2019. The comparator in
  # utils/custom_methods.filter_timeoffs_based_on_hire_date supports only
  # strict ">" / "<" (no ">="), so we use "> Dec 31 2018" to make 1-Jan-2019
  # itself match. This also closes the boundary gap with the Grandfathered
  # rules below (which use "< Jan 1 2019"), so no hire date falls between
  # the two buckets. Do not change to "Jan 1 2019" without first adding
  # ">=" support to the comparator.
  {
    "Country": "philippines",
    "Date of joining compare action": ">",
    "Date of Joining": "Dec 31 2018",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Annual Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": ">",
    "Date of Joining": "Dec 31 2018",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Sick Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Male",
    "Timeoff Type Name": "PHL - Paternity Leave",
    "Should Disabled After Assignment": "No",
    "Marital Status Required": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Parental Leave for Solo Parents",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Emergency Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Compassionate Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Volunteer Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Examination Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Shared Maternity Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Leave without Pay",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": 8,
    "Gender": "All",
    "Timeoff Type Name": "PHL - Compensatory Day Off",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": 9,
    "Gender": "All",
    "Should Disabled After Assignment": "No",
    "Timeoff Type Name": "PHL - Compensatory Day Off"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": "<",
    "Date of Joining": "Jan 1 2019",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Annual Leave Grandfathered",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": "<",
    "Date of Joining": "Jan 1 2019",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Sick Leave Grandfathered",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Female",
    "Timeoff Type Name": "PHL - Maternity Leave",
    "Should Disabled After Assignment": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Female",
    "Timeoff Type Name": "PHL - Special Leave for Women",
    "Should Disabled After Assignment": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "PHL - Unpaid Leave of Absence",
    "Should Disabled After Assignment": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Female",
    "Timeoff Type Name": "PHL - Maternity Leave - Solo Parent",
    "Should Disabled After Assignment": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Female",
    "Timeoff Type Name": "PHL - Maternity Leave - Leave without Pay",
    "Should Disabled After Assignment": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Male",
    "Timeoff Type Name": "PHL - Paternity Leave Extended",
    "Should Disabled After Assignment": "Yes"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "Female",
    "Timeoff Type Name": "PHL - Family Leave",
    "Should Disabled After Assignment": "No"
  },
  {
    "Country": "philippines",
    "Date of joining compare action": null,
    "Date of Joining": "All",
    "Job Level": "All",
    "Gender": "All",
    "Timeoff Type Name": "Holiday",
    "Should Disabled After Assignment": "No"
  }
]