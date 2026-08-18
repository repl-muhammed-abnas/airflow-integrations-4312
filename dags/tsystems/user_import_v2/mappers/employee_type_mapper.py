# Usage Guide for employee_type_mapper:
#
# Field Explanations:
# 1. org_structure_code: Organization structure code (required match)
# 2. work_relationship: "Employee" or "Freelancer" (required match)
# 3. employment_type_include: [] = Allow all employment types, [list] = Allow only these types
# 4. employment_type_exclude: [] = No exclusions (default), [list] = Exclude these types
# 5. employment_subtype_include: [] = Allow all subtypes, [list] = Allow only these subtypes
# 6. employment_subtype_exclude: [] = No exclusions (default), [list] = Exclude these subtypes
# 7. manager_flag: [] = Allow any manager flag value (Yes/No), [list] = Allow only these values
# 8. employee_type: Resulting employee type to assign
#
# Logic Rules:
# - Empty arrays ([]) mean "allow all values" for that field
# - Include lists take precedence over exclude lists
# - If include list is not empty, value must be in the include list
# - If include list is empty, value must NOT be in the exclude list
# - All conditions must match for a rule to apply
# - First matching rule wins (order matters)
#
# Sections:
# - "standard": Normal mapping rules
# - "exceptions": Special preservation rules for UPDATE scenarios only

employee_type_mapper = {
    "standard": [
        # Organization 0370
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0377
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 1046
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0472
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MC", "MD"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        # Organization 6206
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        # Organization 6207
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        # Organization 6208
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        # Organization 0472
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6206 - B, C, D types
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6207 - B, C, D types
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6208 - B, C, D types
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0382
        {
            "org_structure_code": "0382",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0382",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0383
        {
            "org_structure_code": "0383",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0383",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0384
        {
            "org_structure_code": "0384",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0384",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0385
        {
            "org_structure_code": "0385",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0385",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0386
        {
            "org_structure_code": "0386",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0386",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0387
        {
            "org_structure_code": "0387",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0387",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0388
        {
            "org_structure_code": "0388",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0388",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 1048
        {
            "org_structure_code": "1048",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1048",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6205
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6205",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6209
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6209",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6210
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6210",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0010
        {
            "org_structure_code": "0010",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0010",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0010",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0010",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0010",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0010",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0010",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0030
        {
            "org_structure_code": "0030",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0030",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0030",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0030",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0030",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0030",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0030",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0070
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0070",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0150
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0150",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0151
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0151",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0152
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0152",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0153
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0153",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0154
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0154",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0155
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0155",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0156
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0156",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0157
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0157",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "K", "M", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0170
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0170",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0193
        {
            "org_structure_code": "0193",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Employee",
            "employment_type_include": ["H"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Freelancer",
            "employment_type_include": ["W"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0193",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "H", "J", "L", "M", "W", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0250
        {
            "org_structure_code": "0250",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0250",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0250",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0250",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0250",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0250",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0330
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0330",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "E", "K", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0350
        {
            "org_structure_code": "0350",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0350",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0350",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0350",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0350",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0350",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0353
        {
            "org_structure_code": "0353",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0353",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0353",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0353",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0353",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0353",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0430
        {
            "org_structure_code": "0430",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0430",
            "work_relationship": "Employee",
            "employment_type_include": ["K"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0430",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0430",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0430",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0430",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0430",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "K", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0450
        {
            "org_structure_code": "0450",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0450",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0450",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0450",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0450",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0450",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0490
        {
            "org_structure_code": "0490",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0490",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0490",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0490",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0490",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0490",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0820
        {
            "org_structure_code": "0820",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0820",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0820",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0820",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0820",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0820",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0820",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0830
        {
            "org_structure_code": "0830",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0830",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0830",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0830",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0830",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0830",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0830",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0850
        {
            "org_structure_code": "0850",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0850",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0850",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0850",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0850",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0850",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0850",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 0880
        {
            "org_structure_code": "0880",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0880",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0880",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0880",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0880",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0880",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 1183
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1183",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 1440
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["G"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Freelancer",
            "employment_type_include": ["X"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1440",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "D", "E", "G", "J", "L", "M", "X", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 1539
        {
            "org_structure_code": "1539",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1539",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1539",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1539",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1539",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1539",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1539",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 1709
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1709",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "E", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 2600
        {
            "org_structure_code": "2600",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2600",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2600",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2600",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2600",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "2600",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "2600",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "D", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 2641
        {
            "org_structure_code": "2641",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2641",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2641",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2641",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "2641",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "2641",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 2804
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": ["1"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": ["X"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["EI"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Freelancer",
            "employment_type_include": ["X"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["EX"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "2804",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["1", "A", "J", "L", "M", "X", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6201
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6201",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 6229
        {
            "org_structure_code": "6229",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6229",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6229",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "6229",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6229",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6229",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 8344
        {
            "org_structure_code": "8344",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "8344",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "8344",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "8344",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "8344",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "8344",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization 9973
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "9973",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "C", "D", "J", "L", "M", "E", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },

        # Organization Dummy
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["J"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["1"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Freelancer",
            "employment_type_include": ["X"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Freelancer",
            "employment_type_include": ["W"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "Dummy",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "1", "M", "E", "X", "W", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        # Organization 2654
        {
            "org_structure_code": "2654",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Employee",
            "employment_type_include": ["B"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Employee",
            "employment_type_include": ["C"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Employee",
            "employment_type_include": ["D"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Freelancer",
            "employment_type_include": ["E"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Freelancer",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "2654",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": ["A", "B", "C", "D", "J", "L", "E", "M", "F"],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        }
    ],
    
    "exceptions": [
        # Internal Shift Worker exceptions
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift workers"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift workers"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift workers"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift workers"
        },
        
        # Internal Worktype exceptions
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        
        # HR200 Integration exceptions
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 integr. RZ"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 tariffrei"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 integr. RZ"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 tariffrei"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 integr. RZ"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 tariffrei"
        },

        # Int HR200 Tarif exceptions
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],
            "employment_type_exclude": [],
            "employment_subtype_include": [],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif"
        }
    ]
}