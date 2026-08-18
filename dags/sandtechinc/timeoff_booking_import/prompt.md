Create a Time Off Booking Import project in the below path 
airflow-integrations\dags\sandtechinc\timeoff_booking_import

the functional design is as follows
●	The Time Off bookings will be imported from HiBOB on a daily basis. The Integration logic would be set up to pick up files from the SFTP in real-time, ensuring timely updates.
●	Time Off bookings are added/modified in HiBOB
●	Time Off types named identically to those in HiBOB will be created in Polaris and assigned to users accordingly
●	Each Time Off booking entry will have a unique Request ID created in HiBOB
●	The import feed file will be placed on the SFTP server hosted by Replicon
●	Users will have an 8-hour schedule per day, but Time Offs can include half-days. The last day of multiple-day bookings is considered partial based on the hours shared.
●	The client will send the Full File and each file they send will include some data from the previous file and hence, to avoids the duplicate bookings being created on the instance, there would be a reference logic in the integration that compares the previous file with the current one, which would take care of most duplicates + a check to compare the Request ID received for the records which will cover the rest.
●	sftp input file path 


Field Mappings are as follows 

HiBOB  -  DELTEK | REPLICON
Email	-   Login Name
Original request ID	-   Request ID (Custom Field)
Policy Type	-   Time Off Type Name
Start Date	-   Start Date
End Date	-   End Date
Duration	-   Time off Hours (Float Values)
Unit	-   No change with respect to Integration
Change Type	-   Status (Please refer to the mapper below)
Status	-   Status (Please refer to the mapper below)
Updated on	-   No change with respect to Integration
App	-   No change with respect to Integration
Approvers	-   No change with respect to Integration



Change Type in HiBOB	-   Status in HiBOB	 -   Status in Polaris
Request approved	-   Approved	-   Time Off Status to be changed to Approved
Request submitted	-   Pending Approval	-   Time Off should be added in Polaris and the Status should be Waiting for Approval
Request updated (used when someone changes their dates)	-   Pending Approval	-   Time Off should be Updated in Polaris and status should be Waiting for Approval
Request cancelled	-   Cancelled	-   Time Off should be Deleted in Polaris


The headers of input file will be 
Email,Original request ID,	Policy type,	Start date,	End date,	Duration,	Unit,	Change type,	Status,	Updated on,	App,	Approvers

To refer to a previous project use the below project paths
 airflow-integrations\dags\data_intellect_services\timeoff_sync_v1


Refer to 
airflow-integrations\dags\darkmattertechnologiesllc\user_sync_v1\main_dag.py
for reference file logic in the project.

Instead of airflow library use rail library from the below path
replicon-airflow-library