HAMbot
======

Health And Monitoring aka Data Quality Subsystem

##What is it:
A core set of services that allows the automation of data quality checks:

* For any given a job a number of data quality steps will be defined
* Metadata for data quality will be stored in a database  (handling for dynamo and mysql)
* Trap, log and alert assertions
* Checks will be compared against thresholds (each check can have multiple thresholds)
* These thresholds can trigger different alert types  (log alert level, sms)
* Data quality steps will be written in python or SQL (at least initially)

##Data Model:    

job   
* job_id	 
* job_name	 

jobs_checks  
* job_id	 
* check_id    
* check_name  
* business name  
* check_module  
* module path  
  
job_check_thresholds  
* check_id	   
* check_type	- %, amt  
* lower_threshold	   
* upperthreshold	 
* notification - who gets notified and by what means, for now sns  

job_check_log
* log_id	 
* check_date	 
* check_id	 
* threshold_value	 
