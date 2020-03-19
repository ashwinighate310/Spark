#              -  Get data from following url
#					*"http://hklpathas02.hk.standardchartered.com:8088/ws/v1/cluster/apps?user=ocirappdev"
#
#              -  Inserting data into t3_ocir_open.cool_yarn_logs
#                 Usage :
#                 ./yarnLog.sh 
#				  Example :
#                  sh -vx yarnLog.sh ocirappdev
#
# History:
# Created By: Bakul 
#===========================================================================================================

#!/bin/ksh

. /CTRLFW/OCIR/data/yarn_logs/envproperties_test.cfg

#Environment Path
log_path=/CTRLFW/OCIR/data/yarn_logs/
jar_path=/CTRLFW/OCIR/data/yarn_logs/
config_Path=/CTRLFW/OCIR/data/yarn_logs/
file_path=/CTRLFW/OCIR/data/yarn_logs/

#File patih
log_file=${log_path}/"yarn_log`date -u +'%Y%m%d%H%M%S'`.log"
start_time_file=${config_Path}/startTime_test.txt
start_timebkp_file=${config_Path}/startTimebkp_test.txt

#Clear old log files
rm -f ${log_path}/*.log
  
#CHECKING THE INPUT PARAMETERS
if [ $# -lt 1 ]
then
  echo "Error, Not Enough Arguments."
  echo "Usage : `basename $0` [odate] "
  userName=ocirappdev
  #exit 1
fi

#TAKING THE INPUT VARIABLES VALUE
#userName=$1


if [ $# -eq 2 ]
then
  echo "setting user and startTime as per input"
  START_TIME=$2
else
  #Start Time
  START_TIME=`cat $start_time_file`
fi

max_attempts=24
attempt_num=1

while [ ${attempt_num} -le ${max_attempts} ]; do
  #take backup of the current start time
  cat ${start_time_file} >> ${start_timebkp_file}

  #Start Time
  START_TIME1=`cat $start_time_file`
  START_TIME=$(date +%s)
  echo ${START_TIME} >> $log_file

  export SPARK_MAJOR_VERSION=2


  #Spark-JOB to fetch the API data and load it into table
  spark-submit --class ${COOL_SPARK_JOB_CLASS_NAME} --master yarn --driver-memory ${SPARK_DRIVER_MEMORY} --executor-cores ${SPARK_NUM_CORES} --executor-memory ${SPARK_EXEC_MEMORY} --num-executors ${SPARK_NUM_EXECUTORS} --conf spark.port.maxRetries=50 --conf spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS} --conf spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}  --jars ${HIVE_JARS_PATH} $jar_path/${COOL_JAR_NAME} ${START_TIME1} ${userName} 2>&1 | tee ${log_file}

  if [ $? -eq 0 ]; then
    echo " API data loaded successfully for tracking " >> ${log_file}
    #date_time=$(grep "CURRENT TIME" ${log_file} | cut -d\| -f2)
    #prepare starttime file for next run
    grep "CURRENT TIME" ${log_file} | cut -d\| -f2 > ${start_time_file}
  else
    echo "API data load failed" >> ${log_file}
    #exit 1
  fi

  END_TIME=$(date +%s)
  DIFF_TIME=$(( ${END_TIME} - ${START_TIME} ))
  echo "END_TIME: " ${END_TIME} >> ${log_file}
  echo "Total time taken: " ${DIFF_TIME} >> ${log_file}
  attempt_num=$((attempt_num+1))
  sleep 30m
done

df -h > ${file_path}/test_dfdata.txt
hadoop fs -df -h > ${file_path}/test_hdfsdata.txt
sed 's/  */,/g' ${file_path}/test_dfdata.txt > ${file_path}/test_dfdata2.txt
sed 's/  */,/g' ${file_path}/test_hdfsdata.txt > ${file_path}/test_hdfsdata2.txt

START_TIME=$(date +%s)
echo ${START_TIME} >> $log_file

file1=${file_path}/test_dfdata2.txt
file2=${file_path}/test_hdfsdata2.txt

export SPARK_MAJOR_VERSION=2
#Spark-JOB to fetch the API data and load it into table
spark-submit --class ${COOL_SPARK_JOB_DF_NAME} --master yarn --driver-memory ${SPARK_DRIVER_MEMORY} --executor-cores ${SPARK_NUM_CORES} --executor-memory ${SPARK_EXEC_MEMORY} --num-executors ${SPARK_NUM_EXECUTORS} --conf spark.port.maxRetries=50 --conf spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS} --conf spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}  --jars ${HIVE_JARS_PATH} $jar_path/${COOL_JAR_NAME} ${file1} ${file2} 2>&1 | tee ${log_file}

if [ $? -eq 0 ]; then
  echo " DF and HDFS data loaded successfully for tracking " >> ${log_file}
  #date_time=$(grep "CURRENT TIME" ${log_file} | cut -d\| -f2)
  #prepare starttime file for next run
  #grep "CURRENT TIME" ${log_file} | cut -d\| -f2 > ${start_time_file}
else
  echo "DF and HDFS data load failed" >> ${log_file}
  #exit 1
fi

END_TIME=$(date +%s)
DIFF_TIME=$(( ${END_TIME} - ${START_TIME} ))
echo "END_TIME: " ${END_TIME} >> ${log_file}
echo "Total time taken: " ${DIFF_TIME} >> ${log_file}


exit 0
