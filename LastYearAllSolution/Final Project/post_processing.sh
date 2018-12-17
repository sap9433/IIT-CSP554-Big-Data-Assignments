#! /bin/sh

##################################################################################
# User defined function declarations
##################################################################################
copy_files_local()
{
	# Copying the target dataset from HDFS target directory to Linux Server 
	
	echo -e "\n\n`date +%Y-%m-%d` : STEP 1: Copying the files from HDFS to Linux Server..."
	hdfs dfs -ls /user/maria_dev/final_project/target_table
	hdfs dfs -copyToLocal /user/maria_dev/final_project/target_table/part*.csv /home/maria_dev/final_project/target_table.csv
	
	chmod 755 /home/maria_dev/final_project/target_table.csv
	ls -ltr 
	
	# Call next function [STEP 2]
	hive_queries
}

hive_queries()
{
	# Execute HQL commands from the script project_hive_queries.hql
	
	echo -e "\n\n`date +%Y-%m-%d` : STEP 2: Execute analytical queries in HQL over the Hive target_table [IMDB_Movies] to gather Stylized facts..."
	hive –f /home/maria_dev/final_project/project_hive_queries.hql

	echo -e "\n\n`date +%Y-%m-%d` : $0 completed successfully !!!"
}
##################################################################################
# Mainline Processing starts here
##################################################################################
# Setting the script global parameters
##################################################################################

# Reset the display screen
clear

# Call STEP 1: Copy target dataset from HDFS to Linux Server
copy_files_local