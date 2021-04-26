#!/usr/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row, SQLContext

import pandas as pd, numpy as np
from datetime import datetime, timedelta
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import yaml
import json
import os
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def puppet_api_token():
	global table_name
	global records_count
	global end_time
	global status
	global duration
	try:
		odata = {"login":"srv.patch.001", "password": "test123#"}
		header1 = {'Content-Type': 'application/json'}
		token_url= "https://serverapp1158.sg.dev.net:4433/rbac-api/v1/auth/token"
		
		access_token_response = requests.post(url=token_url, headers=header1,data=json.dumps(odata), verify=False)
		access_token = access_token_response.json()["token"]
		log_message("Puppet API Token", "Created Successfully")
		return access_token
		
	except Exception as err:
		status="Error"
		log_message("ERROR - Puppet API Token ", str(sys.exc_info()[0]) + ", " + str(err))
		log_table_entry(end_time, duration, table_name, records_count, status)
		return 1
	

def puppet_api_data(source):
	global table_name
	global records_count
	global end_time
	global start_time
	global status
	global duration
	
	try:
		access_token = puppet_api_token()
		headers = {'X-Authentication': access_token, 'Content-Type': 'application/json'}
		
		url = "https://serverapp1158.sg.dev.net:8081/pdb/query/v4/package-inventory"
		
		if source=='solaris':
			payload = """{"query": ["in", "certname",["extract", "certname",["select_facts", ["and", ["=", "name", "operatingsystem"],["=", "value", "Solaris"]]]]]}"""
			table_name = "puppet_solaris"
		elif source=='aix':
			payload = """{"query": ["in", "certname",["extract", "certname",["select_facts", ["and", ["=", "name", "operatingsystem"],["=", "value", "AIX"]]]]]}"""
			table_name = "puppet_aix"
		else:
			status="Error"
			log_message("Puppet API Source", "Given Source parameter is wrong")
			log_table_entry(end_time, duration, table_name, records_count, status)
			return 1
			
		response = requests.post( url, headers=headers, data = payload,verify=False)
		json_data = json.loads(response.text)
		
		if not json_data[0]:
			records_count = 0
		else:
			try:			
				df_api_solar=sqlContext.createDataFrame(Row(**x) for x in json_data)
				
				schema_name="e2_patch_open"
				
				df_api_solar = df_api_solar.withColumn('hostname', split(df_api_solar['certname'], '\.')[0])
				
				df_load_solar = df_api_solar.select("hostname","package_name","version","provider")
				
				print "Data Loading started in table {0} for {1} ".format(table_name,reporting_date)
				
				df_load_solar.withColumn("reporting_date",lit(reporting_date)).write.mode("overwrite").insertInto(schema_name+"."+table_name,overwrite=True)
				records_count = df_load_solar.count()
				
			except Exception as err:
				status="Error"
				log_message("ERROR - Table Load", str(sys.exc_info()[0]) + ", " + str(err))
				log_table_entry(end_time, duration, table_name, records_count, status)
				return 1
				
			print "Total no of records loaded in table {0}- {1}".format(table_name,records_count)
			
			curr_time = datetime.now()
			end_time = curr_time - timedelta(microseconds=curr_time.microsecond)
	
			duration = (end_time-start_time).seconds
			log_message(source,"--Puppet API Token Job successfully ended--", end_time)
			status="OK"
			log_table_entry(end_time, duration, table_name, records_count, status)
			spark.stop()
	except Exception as err:
		status="Error"
		log_message("ERROR - Table Load", str(sys.exc_info()[0]) + ", " + str(err))
		log_table_entry(end_time, duration, table_name, records_count, status)
		return 1		
		
def log_table_entry(end_time, duration, table_name, records_count, status):
	try:
		global start_time
		global method
		global process_name
		global reporting_date
		global data_source
		global module
		
		spark.sql("insert into e2_patch_open.sourcing_logs values ( '{0}','{1}',{2},'{3}','{4}','{5}','{6}',{7},'{8}','{9}','{10}') ".format(start_time, end_time, duration,method, process_name,module, table_name, records_count, status,data_source,reporting_date))
	except Exception as err:
		status="Error"
		log_message("ERROR - Log Table Load", str(sys.exc_info()[0]) + ", " + str(err))
		return 1

def log_message(module, description, date_time=None):
	try:
		if date_time is None:
			date_time = str(datetime.now().strftime(DATA_DATE_FORMAT))                            
		print date_time , " - Log Message",str(module),str(description)
		return 0
	except Exception as err:
		status="Error"
		print("Unexpected error [log_message]: " + str(sys.exc_info()[0]) + ", " + str(err))
		return 1

def cur_date_time():
	return str(datetime.now().strftime(DATA_DATE_FORMAT))
	
if __name__ == "__main__":
	source= sys.argv[1]
	method="API"
	process_name= "Puppet-"+ source
	data_source= "Puppet"
	module= source
	table_name = ""
	status=""
	
	DATA_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
	
	curr_time = datetime.now()
	start_time = curr_time - timedelta(microseconds=curr_time.microsecond)
	
	reporting_date=datetime.today().strftime('%Y-%m-%d')
	end_time= start_time
	duration= -1
	records_count = -1
	
	log_message(source,"--Puppet API Token Job started--",start_time)
	
	try:
		spark = SparkSession.builder.master("yarn").appName('puppet_aix_solaris').enableHiveSupport().getOrCreate()
		spark.conf.set("hive.exec.dynamic.partition", "true")
		spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
		log_message("PySpark Job Started", "Spark Session is created")
		sqlContext = SQLContext(spark)
	except Exception as err:
		status="Error"
		log_message("END Job1 - ERROR", str(sys.exc_info()[0]) + ", " + str(err))
		exit(1)	
		
	puppet_api_data(source)
	
	
