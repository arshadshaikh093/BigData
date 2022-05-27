package org.programming.exercise

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window

object RollingAverage {
  
  def main(args: Array[String]) {
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    
    val users = Seq((1, "Matt", "user"),
                    (2, "John", "user"),
                    (3, "Louis", "admin")).toDF("id","name","user_type")
                    
    users.show
    
    users.createOrReplaceTempView("users")
    val traffic = Seq((1, "2019-05-01", 15),
                      (2, "2019-05-02", 20),
                      (2, "2019-05-03", 10))toDF("user_id","visited_on","time_spent")
                      
    traffic.show
    traffic.createOrReplaceTempView("traffic")
    
    val windowSepc = Window
                            .orderBy("visited_on")
                            //.rowsBetween(Window.unboundedPreceding, Window.currentRow)
                            
    val joinedDF = users.alias("u").join(traffic.alias("t"), expr("u.id = t.user_id"))
                      .where("u.id = 1 or u.id = 2")
    
    joinedDF.select(col("visited_on"),avg("time_spent").over(windowSepc)).show
    
    spark.sql("""select t.visited_on, avg(t.time_spent) over(order by(t.visited_on) rows between unbounded preceding and current row)
                  from users u join traffic t on u.id = t.user_id
                  where u.id <> 3 
      """).show
    
  }
}