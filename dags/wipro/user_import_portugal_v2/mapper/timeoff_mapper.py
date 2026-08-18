# pylint: disable=line-too-long
time_off_types = {
    "not_acquired": {
        "with_no_children": {
            "local_hire": {
                "female": [
                    "POR - Baixa Medica Gravidez de Risco (Clinic Risk Pregnancy)",
                    "POR -Licença por aborto interrupção da gravidez (Miscarriage Leave)",
                    "POR -Licença parental inicial 42 dias (mãe) (Initial Parental Leave 42 days) (Mother)"
                ],
                "male": [
                    "POR - Licença parental inicial 7 dias (Initial Parental Leave 7 days)",
                    "POR -Licença parental inicial (pai) (Initial Parental Leave) (Father)"
                ],
                "all": [
                    "POR -Baixa Medica (Sickness Leave)",
                    "POR -Licença parental alargada (Extended Parental Leave)",
                    "POR -Assistência familiar para parceiro cônjugue  ou parente (Family Assistance for Partner or Relative)",
                    "POR -Assistência familiar para deficientes ou doentes cronológicos (Family Assistance for Handicap or Chronologically ill)",
                    "POR -Licença de casamento (Marriage leave)",
                    "POR -Licença de luto (morte na gestação)  (Bereavement leave (Gestation Death)"
                ]
            },
            "assignee": {
                "female": ["POR -Licença de maternidade Expatriados (Maternity leave for assignees)"],
                "male": ["POR -Licença de paternidade Expatriados (Paternity leave for assignees)"],
                "all": [
                    "POR - Baixa Medica Expatriados (Sickness Leave Assignee)",
                    "POR -Licença por adoção / Barriga de aluguel Expatriados (Adoption/Surrogacy Leave for assignees)"
                ]
            },
            "local_hire_or_assignee": {
                "female": [
                    "POR -Licença parental inicial (mãe) (Initial Parental Leave) (Mother)"
                ],
                "male": [],
                "all": [
                    "POR -Licença parental Licença para avós (Grandparental leave)",
                    "POR -Licença por adoção (Adoption leave)",
                    "POR -Licença por adoção - Filhos múltiplos várias crianças ( Adoption leave - Multiple children)",
                    "POR -Consulta médica (Medical Appointment)",
                    "POR -Licença de luto (morte falecimento do cônjuge/filhos/genro/nora) (Bereavement leave (Death of Spouse/Child/Son-in-Law/Daughter-in-Law))",
                    "POR -Licença de luto (morte falecimento dos pais, dos sogros e dos padrastos) (Bereavement leave (Death of Parents/Parents-in-Law/Step-Parents))",
                    "POR -Licença de luto (morte falecimento de avós/netos/tios/sobrinhos/sobrinhos) (Bereavement leave (Death of Grand Parents/Grand Children/Uncle/Niece/Nephews))",
                    "POR -Cuidadores informais (Informal Caregivers)",
                    "POR -Cuidadores informais Cuidados familiares com 15 dias (Informal Caregivers Family Care with 15 days)",
                    "POR -Deveres legais (Legal duties)",
                    "POR -Trabalhador-estudante Licença remunerada 2 dias (Student Worker Paid Leave 2 days)",
                    "POR -Licença prolongada para estudantes (Student Extended Leave)",
                    "POR -Licença sabática/perda de remuneração (Sabbatical/Loss of Pay leave)",
                    "POR -Dia de folga compensatório Dia de compensação (Compensatory Day Off)",
                    "POR -Dia de férias compensação de Natal/Fim de ano (Christmas / Year End Leave Day)",
                    "POR -Falta Injustificada (Unjustified absence)",
                    "POR -Falta justificada (Justified absence)",
                    "POR -Acidente de trabalho (Accident at work)",
                    "POR -Realocação / Transferência (Relocation leave)",
                    "POR - Licença de jardinagem|Garden Leave",
                    "POR - Férias anuais transitadas (Annual Leave Carried Over)",
                    "POR - Férias anuais vencidas (Annual Leave Lapsed)",
                    "Bank Holiday",
                ],

            }
        },
        "not_travelport": ["POR - Férias (Annual Leave)"],
        "with_children": {
            "local_hire": {
                "female": [],
                "male": [],
                "all": [
                    "POR -Assistência familiar para crianças com menos de 12 anos. (Family Assistance for child below 12 Years.)",
                    "POR -Assistência familiar para crianças com mais de 12 anos. (Family Assistance for child above 12 Years)",
                ]
            },
            "assignee": {
                "female": [],
                "male": [],
                "all": []
            },
            "local_hire_or_assignee": {
                "female": [
                    "POR -Licença de amamentação - Criança com menos de 1 ano ( Nursing Leave - Child Less than 1 year)",
                    "POR -Licença de amamentação - Criança com mais de 1 ano (Nursing Leave - Child more than 1 year)"
                ],
                "male": [

                ],
                "all": [
                    "POR -Reuniões na escola (Meetings at School)"
                ]
            }
        }
    },
    "travelport": {
        "with_no_children": {
            "local_hire": {
                "female": [],
                "male": [],
                "all": ["POR - Férias anuais Travelport (Annual Leave Travelport)"]
            },
            "assignee": {
                "female": [],
                "male": [],
                "all": []
            },
            "local_hire_or_assignee": {
                "female": [],
                "male": [],
                "all": []
            }
        },
        "with_children": {
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
                "all": []
            }
        }
    }
}
