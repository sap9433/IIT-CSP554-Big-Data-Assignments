#! /bin/sh

##################################################################################
# User defined function declarations
##################################################################################
cleanup()
{
	# Cleaning up previous pre-processed files in HDFS directory (if any)
	
	echo -e "\n\n`date +%Y-%m-%d` : STEP 1: Removing previous pre-processed files in HDFS dir..."
	
	hdfs dfs -rm -r /user/maria_dev/final_project/* >/dev/null 2>&1
	hdfs dfs -ls /user/maria_dev/final_project

	# Call next function [STEP 2]
	pre_process
}

pre_process()
{
	# Preprocessing the original data file for correct parsing and formatting in Hadoop-[pyspark]
	
	echo -e "\n\n`date +%Y-%m-%d` : STEP 2: Pre-process the data files to remove header and "" characters in data..."
	
	cd /home/maria_dev/final_project
	
	sed -i 1d IMDB-Movie-Data.csv >/dev/null 2>&1
	sed -i 1d movies_metadata.csv >/dev/null 2>&1
	
	sed -i 's/""//g' IMDB-Movie-Data.csv >/dev/null 2>&1
	sed -i 's/""//g' movies_metadata.csv >/dev/null 2>&1
	
	wc -l *.csv
	
	# Call next function [STEP 3]
	copy_files_hdfs
}

copy_files_hdfs()
{
	# Copying the pre-processed data files from Linux Server to HDFS target directory
	
	echo -e "\n\n`date +%Y-%m-%d` : STEP 3: Copying the files from Linux Server to HDFS..."
	hdfs dfs -copyFromLocal *.csv /user/maria_dev/final_project
	hdfs dfs -ls /user/maria_dev/final_project
	
	hdfs dfs -cat /user/maria_dev/final_project/IMDB-Movie-Data.csv | wc -l
	hdfs dfs -cat /user/maria_dev/final_project/movies_metadata.csv | wc -l
	
	echo -e "\n\n`date +%Y-%m-%d` : $0 completed successfully !!!"
}

##################################################################################
# Mainline Processing starts here
##################################################################################
# Setting the script global parameters
##################################################################################

# Reset the display screen
clear


# Call STEP 1: Cleanup
cleanup