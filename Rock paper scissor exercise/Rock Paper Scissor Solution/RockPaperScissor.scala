package org.programming.exercise

import org.apache.log4j._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, IntegerType}

case class sales_case_class(INVOICE_ID: Integer, CUSTOMER_ID: String, ITEMS: String, COUNT: Integer, BATCH_ID: String)
case class production_case_class(PRODUCTION_UNIT_ID: String, BATCH_ID: String, ITEMS: String, PRODUCED_COUNT: Integer, DISCARDED_COUNT: Integer)

object RockPaperScissor {

  def createSchema (schemaString : String) : StructType = {
    val fields = schemaString.split("\t")
          .map(s => 
                    if(s=="INVOICE_ID")
                      StructField(s, IntegerType, true) 
                    else 
                      StructField(s, StringType, true))
                      
    val schema = StructType(fields)
    return schema
  }
  
  def convertSalesRow (row: Row) : Array[sales_case_class]= {
    
    val strings = row.getAs[String]("ITEMS_SUMMARY")
    val items = strings.substring(1,strings.length()-1).split(",")
    
    val sales = items.map(item => {
                                    val product = item.split(":")
                                    var productName = product(0).trim()
                                    productName = productName.substring(1,productName.length()-1)
                                    sales_case_class(row.getInt(0), row.getString(1), productName, product(1).trim.toInt, row.getString(3))
                                  })
    return sales
                                  
  }
  
  def convertProductionRow (row: Row) : Array[production_case_class] = {
    val pr_Array = row.getAs[String]("ITEMS_PRODUCED")
    val dr_Array = row.getAs[String]("ITEMS_DISCARDED")
    
    val producedRecords = pr_Array.substring(1, pr_Array.length()-1).split(",")
    val discardedRecords = dr_Array.substring(1, dr_Array.length()-1).split(",")
    
    val productionArray = producedRecords.map(item => {
                                              val pr = item.split(":")
                                              var productName = pr(0).trim
                                              productName = productName.substring(1,productName.length()-1)
                                              
                                              val producedCount = pr(1).trim.toInt
                                              val discardedCount = getDiscardedCount(productName, discardedRecords)
                                              production_case_class(row.getString(0), row.getString(1), productName, producedCount, discardedCount)
    })
    return productionArray;
  }
  
  def getDiscardedCount(productName: String, discardedRecords: Array[String]) : Integer = {
    
    return productName match {
      case "scissor" => {
        val dr = discardedRecords(0).split(":")
        dr(1).trim.toInt
      }
      case "paper"  => {
        val dr = discardedRecords(1).split(":")
        dr(1).trim.toInt
      }
      case "rock"  => {
        val dr = discardedRecords(2).split(":")
        dr(1).trim.toInt
      }
      case _ => 0
    }
  }
  
  
  def main(args:Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    
    val complaintsSchemaString = "INVOICE_ID	DEFECTIVE_ITEM"
    val complaintsSchema = createSchema(complaintsSchemaString)
    
    val salesSchemaString = "INVOICE_ID	CUSTOMER_ID	ITEMS_SUMMARY	BATCH_ID"
    val salessSchema = createSchema(salesSchemaString)
    
    val productionSchemString = "PRODUCTION_UNIT_ID	BATCH_ID	ITEMS_PRODUCED	ITEMS_DISCARDED"
    val productionSchema = createSchema(productionSchemString)
    
    val complaints = spark.read.format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .schema(complaintsSchema)
    .load("./src/resources/exercise/Complaints.tsv")
    complaints.createOrReplaceTempView("complaints")
    
    val sales = spark.read.format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .schema(salessSchema)
    .load("./src/resources/exercise/Sales.tsv")
    
    val production = spark.read.format("com.databricks.spark.csv")
    .option("delimiter", "\t")
    .schema(productionSchema)
    .load("./src/resources/exercise/Production_logs.tsv")
    
    
    val salsDS = sales.flatMap(convertSalesRow)
    salsDS.createOrReplaceTempView("sales")
    
    val productionDS = production.flatMap(convertProductionRow)
    productionDS.createOrReplaceTempView("production_logs")
    
    
    ///******************* Report 1 and Report 3 ****************************************************************
    
    val customer_complaints = spark.sql(""" select comp.DEFECTIVE_ITEM, 
                  sale.BATCH_ID,
                  sum(sale.COUNT) as complaints_count
                  from complaints comp inner join sales sale
                  on comp.INVOICE_ID = sale.INVOICE_ID and
                  comp.DEFECTIVE_ITEM = sale.ITEMS 
                  group by comp.DEFECTIVE_ITEM, sale.BATCH_ID
                  
      """)
    customer_complaints.createOrReplaceTempView("customer_complaints")  
    
    val production_defects = spark.sql(""" select PRODUCTION_UNIT_ID, BATCH_ID, ITEMS,
                  sum(PRODUCED_COUNT) as produced_counts,
                  sum(DISCARDED_COUNT) as discarded_counts
                  from production_logs
                  group by PRODUCTION_UNIT_ID, BATCH_ID, ITEMS       
      """)
      
    production_defects.createOrReplaceTempView("production_defects")

    val total_produced_vs_discarded = spark.sql(""" select pd.PRODUCTION_UNIT_ID, pd.ITEMS, 
                  sum(produced_counts) as produced_total,
                  sum(discarded_counts) as discarded_by_QA,
                  sum(nvl(complaints_count,0)) as complaints_by_cust,
                  (sum(discarded_counts) + sum(nvl(complaints_count,0))) as discarded_total
                  from production_defects pd left outer join customer_complaints cc
                  on pd.BATCH_ID = cc.BATCH_ID and pd.ITEMS = cc.DEFECTIVE_ITEM
                  group by pd.PRODUCTION_UNIT_ID, pd.ITEMS
                  order by pd.PRODUCTION_UNIT_ID
                 
      """)
    
    total_produced_vs_discarded.createTempView("total_produced_vs_discarded")
    
    val report1 = spark.sql(""" select PRODUCTION_UNIT_ID, ITEMS, 
      round(discarded_total/produced_total * 100, 2) as defect_percent
      from total_produced_vs_discarded
      order by PRODUCTION_UNIT_ID, ITEMS
      """)
    
    val report3 = spark.sql(""" select PRODUCTION_UNIT_ID, 
      round(sum(discarded_by_QA)/sum(discarded_total) * 100, 2) as percent_detected_defects_by_QA  
      from total_produced_vs_discarded
      group by PRODUCTION_UNIT_ID
      order by PRODUCTION_UNIT_ID
      """)
    
    report1.show
    report3.show
   ///******************************* Report 2 ***********************************************************
    
    val customrer_grp_complaints = spark.sql(""" select 
                  substring(sale.CUSTOMER_ID,1,1) as Customer_Group, count(comp.INVOICE_ID) as Number_of_complaints
                  from complaints comp inner join sales sale
                  on comp.INVOICE_ID = sale.INVOICE_ID
                  and comp.DEFECTIVE_ITEM = sale.ITEMS
                  group by substring(sale.CUSTOMER_ID,1,1)
                  order by substring(sale.CUSTOMER_ID,1,1)
      """)

    val customer_bought = spark.sql(""" select sale.ITEMS, 
                  substring(sale.CUSTOMER_ID,1,1) as Customer_Group, 
                  sum(sale.count) as Item_bought
                  from sales sale
                  group by substring(sale.CUSTOMER_ID,1,1), sale.ITEMS
                  order by substring(sale.CUSTOMER_ID,1,1)
      """)
      
   customrer_grp_complaints.createOrReplaceTempView("customrer_grp_complaints")
   customer_bought.createOrReplaceTempView("customer_bought")
   
   val numbero_of_complaints_df = spark.sql (""" select cc.Customer_Group, cc.Number_of_complaints, cb.ITEMS, cb.Item_bought
     from customrer_grp_complaints cc inner join customer_bought cb
     on cc.Customer_Group = cb.Customer_Group
     """)
 
   numbero_of_complaints_df.createOrReplaceTempView("numbero_of_complaints_df")

   val pivot_Df = numbero_of_complaints_df.groupBy("Customer_Group").pivot("ITEMS").sum()
   pivot_Df.createOrReplaceTempView("pivot_Df")
   
   val report2 = spark.sql(""" select Customer_Group, `paper_sum(Number_of_complaints)` as Number_of_complaints,
                               `rock_sum(Item_bought)` as rocks_bought,
                               `paper_sum(Item_bought)` as paper_bought,
                               `scissor_sum(Item_bought)` as scissor_bought
                               from pivot_Df
     """)
   
   report2.show
   
   ///******************************* Write Reports on csv files ***********************************************************  
   
   report1.repartition(1).write.format("com.databricks.spark.csv")
   .mode("overwrite").option("header", true).save("G:/Hadoop/Spark/My Learning/Rock paper scissor exercise/report1")
   
   report2.repartition(1).write.format("com.databricks.spark.csv")
   .mode("overwrite").option("header", true).save("G:/Hadoop/Spark/My Learning/Rock paper scissor exercise/report2")
   
   report3.repartition(1).write.format("com.databricks.spark.csv")
   .mode("overwrite").option("header", true).save("G:/Hadoop/Spark/My Learning/Rock paper scissor exercise/report3")
   
   spark.stop
    
  }
}