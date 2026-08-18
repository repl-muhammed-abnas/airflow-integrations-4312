# Usage Guide for employee_type_mapper:
#
# Field Explanations:
# 1. org_structure_code: Organization structure code (required match)
# 2. work_relationship: "Employee" or "External" (required match)
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
        # Spain/Portugal Organizations (0370, 0377, 1046)
        # Internal employees
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        
        # Trainees
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        
        # Students
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        
        # External Contractors (MA, MC)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MA"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        
        # External Services (MD)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        
        # External Freelancer (MF)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MF"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Freelancer"
        },
        
        # External Contractors (catch-all for other M subtypes)
        {
            "org_structure_code": "0370",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # Any value except those in exclude
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # Any value except those in exclude
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # Any value except those in exclude
            "employment_subtype_exclude": ["MA", "MC", "MD", "MF"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        
        # T-Systems ICT India (0472)
        # Internal employees
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["A"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal"
        },
        
        # Trainees
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        
        # Students
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        
        # External Contractors (MD)
        {
            "org_structure_code": "0472",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MD"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        
        # External Services (MC)
        {
            "org_structure_code": "0472",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": ["MC"],
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Services"
        },
        
        # External Contractors (catch-all for other M subtypes)
        {
            "org_structure_code": "0472",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # Any value except those in exclude
            "employment_subtype_exclude": ["MC", "MD"],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        
        # German Organizations (6206, 6207, 6208)
        # Int HR200 Tarif
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["A", "J", "L"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 Tarif"
        },
        
        # Students
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],  # Any value except those in exclude
            "employment_type_exclude": ["A", "J", "L", "M", "F"],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Students"
        },
        
        # Trainees
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": ["F"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Trainees"
        },
        
        # External Contractors
        {
            "org_structure_code": "6206",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "External",
            "employment_type_include": ["M"],
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "External Contractors",
        }
    ],
    
    "exceptions": [
        # Internal Shift Worker exceptions
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift Worker"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift Worker"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift Worker"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal Shift Worker"
        },
        
        # Internal worktype exceptions
        {
            "org_structure_code": "0370",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        {
            "org_structure_code": "0377",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        {
            "org_structure_code": "1046",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        {
            "org_structure_code": "0472",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Internal worktype"
        },
        
        # German organization exceptions
        # Int HR200 integr. RZ
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 integr. RZ"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 integr. RZ"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 integr. RZ"
        },
        
        # Int HR200 FZ=AZ
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 FZ=AZ"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 FZ=AZ"
        },
        
        # Int HR200 tariffrei
        {
            "org_structure_code": "6206",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 tariffrei"
        },
        {
            "org_structure_code": "6207",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 tariffrei"
        },
        {
            "org_structure_code": "6208",
            "work_relationship": "Employee",
            "employment_type_include": [],  # All employment types
            "employment_type_exclude": [],
            "employment_subtype_include": [],  # All subtypes
            "employment_subtype_exclude": [],
            "manager_flag": [],
            "employee_type": "Int HR200 tariffrei",
        }
    ]
}