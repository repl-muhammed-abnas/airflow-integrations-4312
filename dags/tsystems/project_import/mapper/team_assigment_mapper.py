# Team assignment department mapping
# Defines which service centers get project access based on accounting area and department
# When accounting area starts with the key AND department matches, assign team_departments
TEAM_ASSIGNMENT_MAPPING = {
    "0370": [{
        "departments": ["0370_ACSL", "0370_CLSE", "0370_CRDL", "0370_GA", "0370_OTHR", "0370_SCRT"],
        "team_departments": "0370_ACSL|0370_CLSE|0370_CRDL|0370_GA|0370_OTHR|0370_SCRT|0377_ACSL|0377_CLSE|0377_CRDL|0377_GA|0377_OTHR|0377_SCRT|1046_CLSE|1046_GA|1046_OTHR|1046_SCRT|1048_DTIN"
    }],
    "0377": [{
        "departments": ["0377_ACSL", "0377_CLSE", "0377_CRDL", "0377_GA", "0377_OTHR", "0377_SCRT"],
        "team_departments": "0370_ACSL|0370_CLSE|0370_CRDL|0370_GA|0370_OTHR|0370_SCRT|0377_ACSL|0377_CLSE|0377_CRDL|0377_GA|0377_OTHR|0377_SCRT|1046_CLSE|1046_GA|1046_OTHR|1046_SCRT|1048_DTIN"
    }],
    "1046": [{
        "departments": ["1046_CLSE", "1046_GA", "1046_OTHR", "1046_SCRT"],
        "team_departments": "0370_ACSL|0370_CLSE|0370_CRDL|0370_GA|0370_OTHR|0370_SCRT|0377_ACSL|0377_CLSE|0377_CRDL|0377_GA|0377_OTHR|0377_SCRT|1046_CLSE|1046_GA|1046_OTHR|1046_SCRT|1048_DTIN"
    }]
}