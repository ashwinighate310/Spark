	USE t3_gp_open;

	CREATE EXTERNAL TABLE IF NOT EXISTS `gp_bef_staging`(
	  `BEF_Function/Business_name` string,
	  `BEF_metric_Name` string,
	  `attribute` string,
	  `value` DOUBLE)
	PARTITIONED BY (
		`year` int,
	  `quarter` string
	  )
	ROW FORMAT DELIMITED
	  FIELDS TERMINATED BY '|'
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.mapred.TextInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  'hdfs://nnscbhaastest/prd/edm/hadoop/ocir/sit/Tier3_OCIR/t3_gp_open/gp_bef_staging';