package com.scb.cib

import org.apache.spark.rdd.RDD
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTimeConstants, Days, LocalDate, DateTime}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import scala.io.Source
import org.apache.spark.sql.functions.unix_timestamp

object DfDatatest {
  
  case class dfData(FILESYSTEM:String, K_BLOCKS:String, USED_SPACE:String, AVAILABLE_SPACE:String, USE_PERCENT:String, MOUNTED_ON:String)
  case class hdfsData(FILESYSTEM:String, SIZE_DISK:String, USED_SPACE:String, AVAILABLE_SPACE:String, USE_PERCENT:String)
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("DFDatatest")
    val sc = new SparkContext(conf)
    val spark = new org.apache.spark.sql.SQLContext(sc)
    val hc = new HiveContext(sc)
    
    import spark.implicits._
    
    var startTime = ""
    var userName = ""
    if (args.size.equals(2)) {
      startTime = args(0)
      userName = args(1)
      } else {
        println("Not enough arguments given. Please try again with required arguments.")
        sys.exit(1)
        }
    

val file1 = "/CTRLFW/OCIR/data/yarn_logs/test_dfdata2.txt"
val line1 = sc.textFile("file://"+file1)
val header1 = line1.first()
val dataNoHeader1 = line1.filter(x => (x != header1))

val file2 = "/CTRLFW/OCIR/data/yarn_logs/test_dfdata2.txt"
val line2 = sc.textFile("file://"+file2)
val header2 = line2.first()
val dataNoHeader2 = line2.filter(x => (x != header2))

val dataSplit1 = dataNoHeader1.map {x =>
                                      val columns = x.split(",")
                                      dfData(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5))}

val dataSplit2 = dataNoHeader2.map {x =>
                                      val columns = x.split(",")
                                      hdfsData(columns(0), columns(1), columns(2), columns(3), columns(4))}

val ds1 = dataSplit1.toDS
val dswithDate1 = ds1.select($"*", current_date().alias("ods"))

val ds2 = dataSplit2.toDS
val dswithDate2 = ds2.select($"*", current_date().alias("ods"))

hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
hc.setConf("hive.exec.dynamic.partition", "true")

dswithDate1.coalesce(1).write.mode("append").partitionBy("ods").format("hive").saveAsTable("t3_ocir_open.nas_logs")
dswithDate2.coalesce(1).write.mode("append").partitionBy("ods").format("hive").saveAsTable("t3_ocir_open.hdfs_logs")

    }
  
}