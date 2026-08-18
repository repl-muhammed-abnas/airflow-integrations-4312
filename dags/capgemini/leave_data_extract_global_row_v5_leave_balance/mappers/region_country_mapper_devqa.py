REGION_COUNTRY_MAPPER = [
    {
        "region": "APAC",
        "region_code": "APAC",
        "schedule_interval": "0 1 * * *",  # 1:00 AM UTC
        "countries": [
            {"country_code": "ALL", "country_list": ["Japan", "Malaysia", "Hong kong", "Philippines", "Singapore", "Taiwan", "Viet nam", "Australia", "New zealand", "China"], "filename_format": "Devqa_APAC_LeaveBalance"}
        ]
    },
    {
        "region": "Middle East",
        "region_code": "ME",
        "schedule_interval": "30 1 * * *",  # 1:30 AM UTC
        "countries": [
            {"country_code": "ALL", "country_list": ["Saudi arabia", "United arab emirates", "Egypt"], "filename_format": "Devqa_MiddleEast_LeaveBalance"}
        ]
    },
    {
        "region": "Europe",
        "region_code": "EU",
        "schedule_interval": "0 2 * * *",  # 2:00 AM UTC
        "countries": [
            {"country_code": "FR", "country_list": ["France"], "filename_format": "Devqa_EU_FR_LeaveBalance"},
            {"country_code": "UK", "country_list": ["United kingdom"], "filename_format": "Devqa_EU_UK_LeaveBalance"},
            {"country_code": "DE", "country_list": ["Germany"], "filename_format": "Devqa_EU_DE_LeaveBalance"},
            {"country_code": "ES", "country_list": ["Spain"], "filename_format": "Devqa_EU_ES_LeaveBalance"},
            {"country_code": "POR", "country_list": ["Portugal"], "filename_format": "Devqa_EU_POR_LeaveBalance"},
            {"country_code": "NOR", "country_list": ["Norway"], "filename_format": "Devqa_EU_NOR_LeaveBalance"},
            {"country_code": "BEL", "country_list": ["Belgium"], "filename_format": "Devqa_EU_BEL_LeaveBalance"},
            {"country_code": "IR", "country_list": ["Ireland"], "filename_format": "Devqa_EU_IR_LeaveBalance"},
            {"country_code": "PL", "country_list": ["Poland"], "filename_format": "Devqa_EU_PL_LeaveBalance"},
            {"country_code": "IT", "country_list": ["Italy"], "filename_format": "Devqa_EU_IT_LeaveBalance"},
            {"country_code": "UKR", "country_list": ["Ukraine"], "filename_format": "Devqa_EU_UKR_LeaveBalance"},
            {"country_code": "ROU", "country_list": ["Romania"], "filename_format": "Devqa_EU_ROU_LeaveBalance"},
            {"country_code": "SWZ", "country_list": ["Switzerland"], "filename_format": "Devqa_EU_SWZ_LeaveBalance"},
            {"country_code": "AU", "country_list": ["Austria"], "filename_format": "Devqa_EU_AU_LeaveBalance"},
            {"country_code": "SWD", "country_list": ["Sweden"], "filename_format": "Devqa_EU_SWD_LeaveBalance"},
            {"country_code": "DEN", "country_list": ["Denmark"], "filename_format": "Devqa_EU_DEN_LeaveBalance"},
            {"country_code": "FIN", "country_list": ["Finland"], "filename_format": "Devqa_EU_FIN_LeaveBalance"},
            {"country_code": "LUX", "country_list": ["Luxembourg"], "filename_format": "Devqa_EU_LUX_LeaveBalance"},
            {"country_code": "NL", "country_list": ["Netherlands"], "filename_format": "Devqa_EU_NL_LeaveBalance"}
        ]
    },
    {
        "region": "North America",
        "region_code": "NA",
        "schedule_interval": "0 3 * * *",  # 3:00 AM UTC
        "countries": [
            {"country_code": "ALL", "country_list": ["USA", "Canada"], "filename_format": "Devqa_NorthAmerica_LeaveBalance"}
        ]
    },
    {
        "region": "ROW",
        "region_code": "ROW",
        "schedule_interval": "30 3 * * *",  # 3:30 AM UTC
        "countries": [
            {"country_code": "ALL", "country_list": ["Morocco", "Tunisia", "Guatemala", "Costa rica", "Mexico"], "filename_format": "Devqa_ROW_LeaveBalance"}
        ]
    }
]
