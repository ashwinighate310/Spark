#!/bin/sh

###################################################################################
#####Script: GP_FILE_PROCESSING.sh                       #####################
#####Usage : sh GP_FILE_PROCESSING.sh date property_file #####################
#####Description: Script to load file to hive table           #####################
#####Deployment Date :                                        #####################
###################################################################################

. /CTRLFW/OCIR/data/GPsourcing/scripts/QUERY_FUNCTIONS.sh

#####checking argumnets####
if [ $# -ne 2 ];
then
echo "please pass date in format yyyymmdd and property file name as argument in sequence"
echo "usage : sh GP_FILE_PROCESSING.sh date property_file"
exit 1
fi

###sourcing property file#########
source /CTRLFW/OCIR/data/GPsourcing/scripts/$2
if [ $? -eq 0 ]; then
echo "sourcing property file success"
else
echo "sourcing property file failed"
exit 1
fi


run_date=$1
echo "----------Executing GP_FILE_PROCESSING script----------------- ">> $logs_path/$file_name"_"$run_date.log
echo "run date is $run_date"

if [[ $source_name == "KPI" || $source_name == "transformation" ]]; then
	LAST_MONTH=`date "+%Y%m01" -d "$run_date -2 month"`
	file_date=$(date "+%Y%m%d" -d "$LAST_MONTH +1 month -1 day")
else
	LAST_MONTH=`date "+%Y%m01" -d "$run_date -1 month"`
	file_date=$(date "+%Y%m%d" -d "$LAST_MONTH +1 month -1 day")
fi


echo "hive db is $hive_db_name" >> $logs_path/$file_name"_"$run_date.log
echo "hive table name is $hive_table_name" >> $logs_path/$file_name"_"$run_date.log
echo "hive table path is $hdfs_path" >> $logs_path/$file_name"_"$run_date.log

year_val=$(date -d "$file_date" '+%Y')
echo "File processing year is $year_val" >> $logs_path/$file_name"_"$run_date.log
if [[ $partition_type == "month" ]];
then
	paritition_val=$(date -d "$file_date" '+%b')
	
elif [[ $partition_type == "quarter" ]];
then
	run_month=$(date -d "$file_date" '+%-m')

	if [[ $run_month == "1" || $run_month == "2" ||  $run_month == "3" ]];
	then
		paritition_val="q1"
	elif [[ $run_month == "4" || $run_month == "5" ||  $run_month == "6" ]];
	then
		paritition_val="q2"
	elif [[ $run_month == "7" || $run_month == "8" ||  $run_month == "9" ]];
	then
		paritition_val="q3"
	else
		paritition_val="q4"
	fi
fi
echo "File processing $partition_type is $paritition_val" >> $logs_path/$file_name"_"$run_date.log

###assigning file name as per the source
if [ $source_name == "transformation" ];
then
	file=$file_name"_"$paritition_val"_"$year_val
else
	file=$file_name"_"$file_date
fi
echo "file name is $file" >> $logs_path/$file_name"_"$run_date.log


###creating hdfs directory for year partition####
hadoop fs -test -d $hdfs_path/year=$year_val
rc1=$?
if [ $rc1 -eq 0 ]; then
echo "Year partition directory exists" >> $logs_path/$file_name"_"$run_date.log
else
hadoop fs -mkdir $hdfs_path/year=$year_val/
if [ $? -eq 0 ]; then
echo "Year partition directory creation success" >> $logs_path/$file_name"_"$run_date.log
else
echo "Year partition directory creation failed" >> $logs_path/$file_name"_"$run_date.log
exit 1
fi
fi

###creating hdfs directory for monthly partition####
hadoop fs -test -d $hdfs_path/year=$year_val/$partition_type=$paritition_val
rc1=$?
if [ $rc1 -eq 0 ]; then
echo "partition_type partition directory exists" >> $logs_path/$file_name"_"$run_date.log
else
hadoop fs -mkdir $hdfs_path/year=$year_val/$partition_type=$paritition_val/
if [ $? -eq 0 ]; then
echo "partition_type partition directory creation success" >> $logs_path/$file_name"_"$run_date.log
else
echo "partition_type partition directory creation failed" >> $logs_path/$file_name"_"$run_date.log
exit 1
fi
fi

## checking existence of file###

hdfs dfs -test -e $hdfs_path/year=$year_val/$partition_type=$paritition_val/$file.txt
rc=$?
if [ $rc -eq 0 ]; then
echo "file exists , removing file" >> $logs_path/$file_name"_"$run_date.log
hadoop fs -rm -r $hdfs_path/year=$year_val/$partition_type=$paritition_val/*
if [ $? -eq 0 ]; then
echo "removing file from HDFS success" >> $logs_path/$file_name"_"$run_date.log
else
echo "removing file from HDFS failed" >> $logs_path/$file_name"_"$run_date.log
exit 1
fi
fi

##copying file from edge node to HDFS######
echo "copying file from edge node to HDFS" >> $logs_path/$file_name"_"$run_date.log
hadoop fs -put $file_path/$file.txt $hdfs_path/year=$year_val/$partition_type=$paritition_val/$file.txt
if [ $? -eq 0 ]; then
echo "copying file from edgenode to HDFS success" >> $logs_path/$file_name"_"$run_date.log
else
echo "copying file from edgenode to HDFS failed" >> $logs_path/$file_name"_"$run_date.log
exit 1
fi

echo "running msck repair" >> $logs_path/$file_name"_"$run_date.log
runQuery "msck repair table $hive_db_name.$hive_table_name"
if [ $? -eq 0 ]; then
echo "msck repair command success" >> $logs_path/$file_name"_"$run_date.log
else
echo "msck repair command failed" >> $logs_path/$file_name"_"$run_date.log
exit 1
fi

####checking hive table count #####
echo "checking hive table count after processing file" >> $logs_path/$file_name"_"$run_date.log
#count=`runQuery "select count(*) from $hive_db_name.$hive_table_name where year='$year_val' and $partition_type='$paritition_val';"`
count=$(runQuery "select count(*) from $hive_db_name.$hive_table_name where year='$year_val' and $partition_type='$paritition_val';")
echo "hive table count is $count" >> $logs_path/$file_name"_"$run_date.log
if [ $count -eq 0 ]; then
echo "hive table count is 0 , check the issue" >> $logs_path/$file_name"_"$run_date.log
exit 1
else
echo "hive table count is non zero and hive table loading success" >> $logs_path/$file_name"_"$run_date.log
fi

###Sending the file to BEF server ###
if [ $source_name == "transformation" ];
then
	scp $file_path/$file.txt $destination_username:$destination_path
fi


echo "----------End of FILE_PROCESSING script----------------- ">> $logs_path/$file_name"_"$run_date.log
##end of script##