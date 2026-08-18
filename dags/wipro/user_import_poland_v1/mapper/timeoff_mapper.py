# pylint: disable=line-too-long
time_off_types = {
    "not_acquired": {
        "local_hire": {
            "female": [],
            "male": [],
            "all": [
                "POL - Urlop wypoczynkowy | Annual leave",
                "POL - Zwolnienie chorobowe dla osób zatrudnionych lokalnie - pracownik | Sick Leave for Local Hire - Self",
                "POL - Zwolnienie opieka - członek rodziny | Sick leave - family member",
                "POL - Zwolnienie opieka - dziecko | Sick leave - child"
            ]
        },
        "assignee": {
            "female": [],
            "male": [],
            "all": [
                "POL - Urlop wypoczynkowy - pracownik oddelegowany | Annual leave - assignee",
                "POL - Zwolnienie chorobowe dla pracownika oddelegowanego do 5 dni - self | Sick Leave for Assignee - Self (5 days)",
                "POL - Zwolnienie chorobowe dla pracownika oddelegowanego powyżej 5 dni - self  | Sick Leave for Assignee - Self (beyond 5 days)"
            ]
        },
        "local_hire_or_assignee": {
            "female": [
                "POL - Urlop macierzyński dla czworaczków | Maternity Leave for quadruplet",
                "POL - Urlop macierzyński dla pięcioraczków lub więcej | Maternity Leave for Quintuplets or more",
                "POL - Urlop macierzyński dla trojaczków | Maternity Leave for triplets",
                "POL - Urlop macierzyński na bliźniaki | Maternity Leave for twins",
                "POL - Urlop macierzyński z tytułu urodzenia jednego dziecka | Maternity Leave for Single child birth"
            ],
            "male": [
                "POL - Urlop ojcowski | Paternity Leave",
                "POL - Urlop okolicznościowy: Urlop z tytułu urodzenia dziecka | Compassionate Leave: Child birth Leave"
            ],
            "all": [
                "POL - Krwiodawstwo | Blood donation",
                "POL - Niebecność usprawiedliwiona niepłatna (stawiennictwo) | Court Duty Leave",
                "POL - Nieusprawiedliwiona nieobecność  | Unexcused absence - absconding",
                "POL - Nieusprawiedliwiona nieobecność - niepłatna | Unexcused absence - Unpaid",
                "POL - Odbiór czasu wolnego za pracę w nadgodzinach | Compensatory off",
                "POL - Służba wojskowa | Military leave",
                "POL - Urlop bezpłatny | Leave Without Pay (LWOP)",
                "POL - Urlop okolicznościowy: ślub dziecka| Compassionate Leave: Marriage Leave - Child",
                "POL - Urlop okolicznościowy: Ślub własny | Compassionate Leave: Marriage Leave - Self",
                "POL - Urlop okolicznościowy związany ze śmiercią krewnych 1. stopnia  | Compassionate Leave 1st Degree Relative",
                "POL - Urlop okolicznościowy związany ze śmiercią krewnych 2. stopnia | Compassionate Leave 2nd Degree Relative",
                "POL - Urlop opiekuńczy (art. 173(1) KP) | Carer leave",
                "POL - Urlop opiekuńczy (art. 188 KP) | Childcare leave",
                "POL - Urlop postojowy | Furlough Leave",
                "POL - Urlop relokacyjny | Relocation Leave",
                "POL - Urlop rodzicielski | Parental leave",
                "POL - Urlop rodzicielski na wielodzietność | Parental leave for multiple child",
                "POL - Urlop wychowawczy | Child raising leave",
                "POL - Usprawiedliwiona nieobecność - niepłatna | Excused absence - unpaid",
                "POL - Usprawiedliwiona nieobecność - płatna | Excused absence - paid",
                "POL - Zwolnienie od pracy - siła wyższa | Force Majeure Leave",
                "POL - Zwolnienie ze świadczenia pracy | Garden Leave",
                "Bank Holiday"
            ]
        }
    }

}

disabled_timeoff_types = {
    "not_acquired": {
        "local_hire": {
            "female": [],
            "male": [],
            "all": []
        },
        "assignee": {
            "female": [],
            "male": [],
            "all": []
        },
        "local_hire_or_assignee": {
            "female": [],
            "male": [],
            "all": [
                "POL - Turnus rehabilitacyjny | Rehabilitation Session Leave",
                "POL - Urlop dla weterana | Veteran Leave",
                "POL - Urlop jubileuszowy | Jubilee Leave",
                "POL - Urlop na poszukiwanie pracy | Looking for a job",
                "POL - Urlop rehabilitacyjny | Disability Leave",
                "POL - Urlop szkoleniowy | Educational leave",
                "POL - Wolontariat | Voluantry Leave"
            ]
        }
    }

}