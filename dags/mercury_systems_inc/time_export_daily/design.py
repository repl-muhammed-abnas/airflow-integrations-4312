#use crl report_to_sftp as the reference folder
# runs at 11 pm everyday
# report filters for program name WO, start and enddate from today -(today-9), approved date today or today-1
# report name =GL_Weekly_Export

# "EntryDateFilter"today -(today-9),"ApprovalDateFilter" approved date today or today-1,"ProgramFilter" is WO
# run the above report populate the below headers into a csv file
# ["EMPLOYEE ID",	
#                     "PROJECT NAME",
#                     "WORK ORDER / PROJECT ID",
#                     "TASK NAME",
#                     "OPERATION / TASK ID",
#                     "POSTING DATE",
#                     "HOURS",
#                     "EMPLOYEE APPROVAL",
#                     "MANAGER APPROVAL",
#                     "EMPLOYEE OU",
#                     "EMPLOYEE CHARGE TYPE",
#                     "FIRST NAME",
#                     "LAST NAME",
#                     "EMPLOYEE DEPARTMENT",
#                     "CHARGE TYPE"
#                     ],
# archive if any older files present and place the new file send the completion mail 