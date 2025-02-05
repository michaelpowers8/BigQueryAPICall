# THIS IS JUST A SKELETON OF A REAL PROJECT I DID AT A PREVIOUS JOB! NO REAL CREDENTIALS ARE BEING USED IN THIS PROJECT, AND IT WILL NOT DO ANYTHING!
# Big Query API Call
This program will attempt to pull a series of tables from Big Query databases, save the pulled data to CSV files and export them for analysis.

## Main functions
To check the available functions, you can run the exe with just a command line prompt of ? It will return the following text:

### Available parameters:
- [-all] - Request all available tables
- [-alldate] - Request all Dated tables
- [-allnodate] - Request all Non-Dated tables
- [-itemdate] - Request all ITEM tables with Dates
- [-itemnodate] - Request all ITEM tables without Dates
- [-allitem] - Request all ITEM tables
- [-impdate] - Request all IMP tables with Dates
- [-impnodate] - Request all IMP tables without Dates
- [-allimp] - Request all IMP tables
- [-pidate] - Request all PI tables with Dates
- [-pinodate] - Request all PI tables without Dates
- [-allpi] - Request all PI tables
- [-test] - Request all available tables and send them to the testing file specified in App.config

### Types of Tables
There are 2 types of tables that data will be extracted from in this program:
 - Dated Tables
		- These tables are often significantly larger, and contain a column with a specified date.
		- Date columns checked in this program as of 2025-02-04 include PARK, INVOICE_DATE, ACTIVITY_DATE, PRODUCT_CREATE_DATE
		- If you attempt to SELECT * from these tables, the program will crash
	- Non-Dated Tables
		- These Tables are smaller and contain summarized information and often do not include dates
		- Anytime data is pulled from these tables, all data is pulled (SELECT * FROM TABLE)

### Start and End Dates
This program can pull tables that require specifying the date. The dates specified in App.config give the program a range of dates to pull data.
The program first checks if an overriding start and end date have been manually set. Overriding dates must be in YYYY-MM-DD format.
If no overriding dates are set, then the program checks for offsetting dates. 
These are negative integers that set the start and end dates n days before today. If the start date is set to -10, and today's date is 2025-01-28,
then the actual starting date will be 2025-01-18.  
The starting date must be before the ending date. If it is not, an error will be sent, and no tables that require dates will be extracted.

### BigQuery Credentials
These credentials are essential to running this job. If these credentials are ever lost, this program cannot run. 
The credentials are saved in two separate json files, and also in App.config as a backup. Credentials are only pulled from
the json files due to Google Big Query API acting weird when trying to pull from App.config.

### Data Extraction
If the passed parameter is -all, then data will be pulled in the order of Non-Dated tables first, then Dated tables after. 
As of 2025-02-04, all data tables pulled are saved to CSV files and those CSV files are immediately sent to C:\BigQueryFinalFiles.
When dated tables are being extracted, the program always pulls every dated table from a single day, and then moves to the next day.
If the passed parameter is -test, then all data will be pulled the same way, but stored to a different location specified in App.config. 
This was used in the creating of this project and should not be used in day to day business.

### Emailing Out Problems
This program is filled with try catch statements. If an exception is caught, the error will be logged, and when the program concludes, 
an email consisting of every error caught will be sent to the designated email in App.config. That email is test@gmail.com.
Other information is also logged throughout the process due to how time consuming this program can be if a large date range of data is being pulled.