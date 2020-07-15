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
import scala.util.Try
import scala.io.Source
import org.apache.spark.sql.functions.unix_timestamp

object YARNLog {
  
  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("YARNLog")
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
    

    val url1 = "http://yarnserver01.com:8088/ws/v1/cluster/apps" + "?user=" + userName + "&startedTimeBegin=" + startTime 
    println("***********************PRINTING URL1 *****************")
    println(url1)
    
    val url2 = "http://yarnserver02.com:8088/ws/v1/cluster/apps" + "?user=" + userName + "&startedTimeBegin=" + startTime 
    println("***********************PRINTING URL2 *****************")
    println(url2)
    
    val result1 = scala.io.Source.fromURL(url1).mkString
    val result2 = scala.io.Source.fromURL(url2).mkString
    
    val jsoneRdd1 = sc.parallelize(result1 :: Nil)
    val jsoneRdd2 = sc.parallelize(result2 :: Nil)
    
    val dateTime = unix_timestamp()
    println("*************************************** CURRENT TIME |" + dateTime.expr.eval() + "|************************")
    
    val ds1 = spark.read.json(jsoneRdd1.toDS).select(explode($"apps.app"))
    val ds2 = spark.read.json(jsoneRdd2.toDS).select(explode($"apps.app"))
    
    //val df1= ds1.select(explode($"apps.app"))
    //val df2= ds2.select(explode($"apps.app"))
    //we can check the column diagnostics is present or not
    
    val df1WithDate = ds1.select($"col.*", current_date().alias("Todays_Date"))
    val df2WithDate = ds2.select($"col.*", current_date().alias("Todays_Date"))
    
    var df1WithDateFinal =
    if (hasColumn(df1WithDate, "resourceRequests")) {df1WithDate} else 
    {df1WithDate.select($"allocatedmb", $"allocatedvcores", $"amcontainerlogs", $"amhosthttpaddress", $"amnodelabelexpression", $"applicationtags", $"applicationtype", $"clusterid", $"clusterusagepercentage", $"diagnostics", $"elapsedtime", $"finalstatus", $"finishedtime", $"id", $"logaggregationstatus", $"memoryseconds", $"name", $"numamcontainerpreempted", $"numnonamcontainerpreempted", $"preemptedresourcemb", $"preemptedresourcevcores", $"priority", $"progress", $"queue", $"queueusagepercentage",lit(null).alias("resourcerequests"),  $"runningcontainers", $"startedtime", $"state", $"trackingui", $"trackingurl", $"unmanagedapplication", $"user", $"vcoreseconds",current_date().alias("Todays_Date"))}
    
    var df2WithDateFinal =
    if (hasColumn(df2WithDate, "resourceRequests")) {df2WithDate} else 
    {df2WithDate.select($"allocatedmb", $"allocatedvcores", $"amcontainerlogs", $"amhosthttpaddress", $"amnodelabelexpression", $"applicationtags", $"applicationtype", $"clusterid", $"clusterusagepercentage", $"diagnostics", $"elapsedtime", $"finalstatus", $"finishedtime", $"id", $"logaggregationstatus", $"memoryseconds", $"name", $"numamcontainerpreempted", $"numnonamcontainerpreempted", $"preemptedresourcemb", $"preemptedresourcevcores", $"priority", $"progress", $"queue", $"queueusagepercentage",lit(null).alias("resourcerequests"),  $"runningcontainers", $"startedtime", $"state", $"trackingui", $"trackingurl", $"unmanagedapplication", $"user", $"vcoreseconds",current_date().alias("Todays_Date"))}
    
    hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hc.setConf("hive.exec.dynamic.partition", "true")
    
    val df2Final = df1WithDateFinal.unionAll(df2WithDateFinal)
      
    df2Final.coalesce(1).write.mode("append").partitionBy("Todays_Date").format("hive").saveAsTable("yarn_db.yarn_api_logs")
    

  }
  
}