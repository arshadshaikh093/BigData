package org.programming.exercise

import org.apache.log4j._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object MultipleJoinsPractice {
  
  val Customer_Table_Name = "customer"
  val Book_Table_Name = "Book"
  val Purchase_Table_Name = "purchase"
  val Cutomer_Address_Table_Name = "cust_Addr"
  
  def createCustomerAddressDataFrame(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    
    val custAddrData = Seq(("C1Arshad Shaikh","A Wing Building"),
                           ("C2Saif Patel","B Wing Building"),
                           ("C3Danish Pandhre","C Wing Building"))
                           
    val custAddresses = spark.sparkContext.parallelize(custAddrData).toDF("customerKey","Address")
    custAddresses
  }
  
  def createCustomerDataFrame(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    val custData = Seq(("C1","Arshad Shaikh", 25, "Hyd", "M"),
                       ("C2","Saif Patel", 26, "Mum", "M"),
                       ("C3","Danish Pandhre", 27, "Mum", "M"))
    val customer = spark.sparkContext.parallelize(custData).toDF("CustID","CustName","Age","City","Gender")
    
    customer  
  }
  
  def createBookDataFrame(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    val bookData = Seq(("B1","Novel"),("B2","Romance"),("B3","Horror"),("B4","SciFi"))
    
    val book = spark.sparkContext.parallelize(bookData).toDF("ISBN","Genere")
    
    book
  }
    
  def createPurchaseDataFrame(spark: SparkSession) : DataFrame = {
   import spark.implicits._
   val purchaseData = Seq((1999,"C1","B1","Amazon",80),
                          (1989,"C1","B2","eBay",60),
                          (1949,"C2","B3","Filpkart",70),
                          (1979,"C2","B4","Myntra",70),
                          (1976,"C3","B1","Amazon",75),
                          (1977,"C3","B3","Myntra",76))
                          
   val purchase = spark.sparkContext.parallelize(purchaseData).toDF("Year","CusomterID","BookISBN","Shop","Price")                        
   purchase
  }
  
  def customerJoinedPurchase(spark: SparkSession): DataFrame = {
    import spark.implicits._
    
    val joinConditions = expr("C.CustID = P.CusomterID")
    
    val joinedResult = spark.table(Customer_Table_Name).alias("C")
    .join(spark.table(Purchase_Table_Name).alias("P"), joinConditions, "inner")
    
    joinedResult.show
    
    joinedResult
  }
  
  def threeJoins(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    
    val newJoinConditions = expr("P.BookISBN = B.ISBN")
    
    val result = customerJoinedPurchase(spark)
      .join(spark.table(Book_Table_Name).alias("B"), newJoinConditions, "inner")
      
    result.show  
    result
  }
  
  def allJoins(spark: SparkSession) : DataFrame = {
    import spark.implicits._
    
    val addrJoinConditions = expr("CONCAT(C.CustID,C.CustName) = Addr.customerKey")
    
    val totalResult = threeJoins(spark)
                        .join(spark.table(Cutomer_Address_Table_Name).alias("Addr"), addrJoinConditions, "inner")
    
    totalResult.show
    totalResult
  }
  
  def checkTwoTableJoin(spark: SparkSession) = {
     val custPurchase = customerJoinedPurchase(spark).selectExpr("C.CustID", "P.CusomterID", "P.Price")
    
    custPurchase.show
  }
  
  def chekcThreeTableJoin(spark: SparkSession) = {
    val selectColms = Seq("C.CustID", "P.CusomterID", "P.Price")
    
    val allCols = selectColms ++ Seq("B.Genere",
        "case when C.CustID = 'C1' then 'Arshad Mac' else C.CustID end as Customer_Name") 
      
   /* val finalResult = allJoins(spark).selectExpr("C.CustID", "P.CusomterID", "P.Price","B.Genere",
        "case when C.CustID = 'C1' then 'Arshad Mac' else C.CustID end as Customer_Name")*/
    
     val finalResult = threeJoins(spark).selectExpr(allCols: _*)
    finalResult.show
  }
  
  def checkFourTableJoin(spark: SparkSession) = {
     
    val selectCols = Seq("C.CustID", "P.CusomterID", "P.Price","B.Genere",
        "case when C.CustID = 'C1' then 'Arshad Mac' else C.CustID end as Customer_Name",
        "CONCAT(Addr.Address,' Mumbai')")
        
    allJoins(spark)
      .selectExpr(selectCols: _*).show(false)
  }
  
  def performAggregationOnFourTableJoin(spark: SparkSession) {
    val selectCols = Seq("count(C.CustID)", "P.CusomterID", "P.Price","B.Genere",
        "CONCAT(Addr.Address,' Mumbai')")
        
    allJoins(spark)
    .selectExpr(selectCols: _*)
    .groupBy(selectCols.tail.head, selectCols.tail.tail: _*)
      //.show(false)
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
    
    val customer = createCustomerDataFrame(spark)
    val book = createBookDataFrame(spark)
    val purchase = createPurchaseDataFrame(spark)
    val cust_Address = createCustomerAddressDataFrame(spark)
    
    customer.show
    book.show
    purchase.show
    cust_Address.show
    
    println("Partitions size on customer " + customer.rdd.partitions.size)
    customer.rdd.glom().collect().foreach(x=>x.foreach(print))
    
    customer.createOrReplaceTempView(Customer_Table_Name)
    book.createOrReplaceTempView(Book_Table_Name)
    purchase.createOrReplaceTempView(Purchase_Table_Name)
    cust_Address.createOrReplaceTempView(Cutomer_Address_Table_Name)
    
    
    /*checkTwoTableJoin(spark)
    chekcThreeTableJoin(spark)
    checkFourTableJoin(spark)
    */
    
      val selectCols = Seq("sum(C.CustID)", "P.CusomterID", "P.Price","B.Genere",
        "CONCAT(Addr.Address,' Mumbai')")
        
        println(selectCols.tail.head)
        println(selectCols.tail.tail)
   
    performAggregationOnFourTableJoin(spark)
   
  }
  
  
}