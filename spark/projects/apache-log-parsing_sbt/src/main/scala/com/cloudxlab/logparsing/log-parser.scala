package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD



object EntryPoint {

val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
val schemaString = "host timeStamp url httpCode bytesTransfered"


val fieldsArray = schemaString.split(" ")
val fields = fieldsArray.map(
f => StructField(f, StringType, nullable = true)
)
val schema = StructType(fields)

def parseLogLine(log: String) :(String,String,String,String,String) = {
val res = PATTERN.findFirstMatchIn(log) 
if (res.isEmpty){
println("Rejected Log Line: " + log)
("Empty", "", "",  "-1", "")
} else {
val m = res.get
return (m.group(1).trim().toUpperCase(), m.group(4).trim().toUpperCase(),m.group(6).trim().toUpperCase(), m.group(8).trim().toUpperCase(), m.group(9).trim().toUpperCase())}
}



    def main(args: Array[String]) {
	
	    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """
	
	        if (args.length != 3) {
            println("Expected:3 , Provided: " + args.length)
            println(usage)
            return;
        }

        // Create a local StreamingContext with batch interval of 10 second
		
  val spark = SparkSession
   .builder()
   .appName("LogParsing")
   .getOrCreate()
   
   
//        val conf = new SparkConf().setAppName("LogParsing")
//        val sc = new SparkContext(conf);
   
spark.sparkContext.setLogLevel("WARN")

val logFile = spark.sparkContext.textFile(args(2))

val rowRDD = logFile.map(parseLogLine).filter(x=> x._1 != "Empty").map(x => Row(x._1,x._2,x._3,x._4,x._5)).persist()

import spark.implicits._

val nsaLogDF = spark.createDataFrame(rowRDD, schema)

val nsaLogDF2 = nsaLogDF.selectExpr("host","timeStamp","url","cast(httpCode as int) httpCode","bytesTransfered")

nsaLogDF2.createOrReplaceTempView("nasa_log")

val output = spark.sql("select * from nasa_log")
spark.sql("cache TABLE nasa_log")


spark.sql("select url,count(*) as req_cnt from nasa_log where url like '%HTML%' group by url order by req_cnt desc LIMIT 10").show
spark.sql("select url,count(*) as req_cnt from nasa_log where url like '%/KSC.HTML%' group by url").show
spark.sql("select substr(timeStamp,1,14),count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show
spark.sql("select substr(timeStamp,1,14),count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt asc LIMIT 5").show
spark.sql("select httpCode,count(*) as req_cnt from nasa_log group by httpCode ").show



    }
}