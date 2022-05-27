package org.programming.exercise


import org.apache.log4j._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_extract

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, IntegerType}

object ApiAnalytics {
  
  def main(args:Array[String]) {
    
  Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C://winutils");
    val spark = SparkSession
      .builder
      .appName("ApiAnalytics")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    
    val readLog = spark.sparkContext.textFile("./src/resources/exercise/elb.log")
    
    val splitRDD = readLog.map(x => x.split(" "))
    val totalCount = splitRDD.map(x => x(12)).count()
    val result = splitRDD.map(x => x(12)).filter(x => x.contains("?"))
    
    val res = result.map(x => x.substring(x.lastIndexOf("/")+1, x.indexOf("?")))
                    .map(x => (x,1))
                    .reduceByKey(_+_)
                    .map(x => (x._1.substring(0,x._1.indexOf(".")),
                               x._2,
                               "%.2f".format(((x._2).toDouble/totalCount)*100).toDouble))
                    .sortBy(_._3, false)
     
                    
   res.collect.foreach(println)
   val resDF = res.toDF()
    
   resDF.select($"_1".alias("Method"), $"_2".alias("Count"), (round(($"_2"/totalCount)*100,2)).alias("Percentage")).show()
    //println(totalCount)
    //println(res.collect().foreach(println))
    
    //val result = splitRDD.map(x => x(12).filter(i => x(12).contains("?"))).collect().foreach(println)
    
    //readLog.printSchema()
    
    /*val split_df = readLog.select(
        regexp_extract(, """\w+\s+([^\s]+)\s+HTTP.*"'""", 1)
        )
    split_df.show()
*/
    //val r = regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1)
    
    /*val counts = readLog.flatMap(line => line.split("""https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,}"""))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
    counts.collect.foreach(println)*/           
  }
}