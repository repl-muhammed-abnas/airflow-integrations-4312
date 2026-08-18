null = None
# pylint: disable=line-too-long
create_final_payroll_data_collection = '''Codigo,cohade,nro,periodo,monto,cencos,perimp,cuotot,obs,codpres,entrydate,status,ampm,tipo,motivo,rebsal,paycodename,Dulic,Detalle,coform,fecha_ini,fecha_fin,propor,moti_mod,simes,Pertom
89531888,BONEDS,1,M,1.6,,,,,,2021-11-13,V,,5,1,N,BONEDS,,,,,,,,,
89531888,BONEDS,1,M,2.6,,,,,,2021-11-23,V,,5,1,N,BONEDS,,,,,,,,,
89531888,BONEDS,1,M,2.6,,,,,,2021-11-25,V,,5,1,N,BONEDS,,,,,,,,,
89531888,BONEDS,1,M,2.6,,,,,,2021-11-26,V,,5,1,N,BONEDS,,,,,,,,,
89531888,BONEDS,1,M,2.6,,,,,,2021-11-27,V,,5,1,N,BONEDS,,,,,,,,,
89531888,BONEDS,1,M,2.6,,,,,,2021-11-29,V,,5,1,N,BONEDS,,,,,,,,,
89531888,BONEDS,1,M,2.6,,,,,,2021-11-30,V,,5,1,N,BONEDS,,,,,,,,,
89531888,10,1,M,7.4,,,,,,2021-11-02,V,,5,1,N,[Chile] Cambio de casa,,,,,,,,,
89531888,10,1,M,7.4,,,,,,2021-11-03,V,,5,1,N,[Chile] Cambio de casa,,,,,,,,,
89531888,11,1,M,7.4,,,,,,2021-11-04,V,,5,1,N,[Chile] cumpleaños,,,,,,,,,
89531888,12,1,M,7.4,,,,,,2021-11-24,V,,5,1,N,[Chile] examenes medicos,,,,,,,,,
89531888,5,1,M,7.4,,,,,,2021-11-12,V,,5,1,N,[Chile] Nacimiento,,,,,,,,,
89531888,6,1,M,7.4,,,,,,2021-11-09,V,,5,1,N,[Chile] Matrimonio,,,,,,,,,
89531888,6,1,M,7.4,,,,,,2021-11-10,V,,5,1,N,[Chile] Matrimonio,,,,,,,,,
89531888,6,1,M,7.4,,,,,,2021-11-09,V,,5,1,N,[Chile] Vacation,,,,,,,,,
89531888,6,1,M,7.4,,,,,,2021-11-10,V,,5,1,N,[Chile] Vacation,,,,,,,,,
89531888,7,1,M,7.4,,,,,,2021-11-22,V,,5,1,N,[Chile] defunción Padres,,,,,,,,,
89531888,8,1,M,7.4,,,,,,2021-11-05,V,,5,1,N,[Chile] defunción Conyuje e hijos,,,,,,,,,
89531888,8,1,M,7.4,,,,,,2021-11-06,V,,5,1,N,[Chile] defunción Conyuje e hijos,,,,,,,,,
'''

report_batch_result = {
    "reportGenerationResults": [
        {
            "error": null,
            "filterValues": [
                {
                    "reportFilterUri": "urn:replicon-tenant:dc2477cce42c427d8f3d41f43c3f1288:report-filter:9be6ba3ae10e4f01a0f250f4f06c6ddb;daterangefilter",
                    "value": null
                },
                {
                    "reportFilterUri": "urn:replicon-tenant:dc2477cce42c427d8f3d41f43c3f1288:report-filter:9be6ba3ae10e4f01a0f250f4f06c6ddb;daterangefilter",
                    "value": "11/02/2021"
                },
                {
                    "reportFilterUri": "urn:replicon-tenant:dc2477cce42c427d8f3d41f43c3f1288:report-filter:9be6ba3ae10e4f01a0f250f4f06c6ddb;daterangefilter",
                    "value": "11/30/2021"
                }
            ],
            "payload": '''RUT,Booking Start Date/Time,Time Off Days,Booking End Date/Time,Time Off Comments,Time Off Type,end time
89531888,22 November 2021,7.00,30 November 2021,,[Chile] Vacation,0
89531888,22 November 2021,6.00,27 November 2021,,[Chile] Vacation,0
89531888,29 November 2021,2.00,30 November 2021,,[Chile] Matrimonio,0
89531888,16 November 2021,5.00,22 November 2021,,[Chile] Matrimonio,0
09050519K,16 November 2021,5.00,22 November 2021,,[Chile] cumpleaños,0
89531888,24 November 2021,2.00,25 November 2021,,[Chile] Vacation,0
}
            ''',
            "reportUri": "urn:replicon-tenant:dc2477cce42c427d8f3d41f43c3f1288:report:363500e0-b0c0-418d-9bf7-ed7a4a2cb495"
        }
    ]
}
