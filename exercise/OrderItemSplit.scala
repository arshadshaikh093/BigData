package org.programming.exercise

import org.apache.log4j._

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class order_case_class(sessionId: String, orderId: String, productId: String, price: Double, tradeDiscount: Double, mrp: Double, Size: String, couponSaving: Double, promotionSaving: Double, qty: Integer)

object OrderItemSplit {

  def convertOrderRow(row: Row): Array[order_case_class] = {
    
    val productString = row.getAs[String]("ListOfProduct")
    var products = Array[order_case_class]()
    if(productString != null) {
          val items = productString.substring(2, productString.length() - 2).split("\\},\\{")
      
          products = items.map(item => {
            val product = item.split(",")
            
            var productID = ""
            var price = 0.0d
            var tradeDiscount = 0.0d
            var mrp = 0.0d
            var size = ""
            var couponSaving = 0.0d
            var promotionSaving = 0.0d;
            var qty = 0;
            val productCol = product.map(p => {
              val cols = p.split("=")
              
              cols(0) match {
                    case "ProductId" => {
                      productID = cols(1)
                    }
                    case "Price" => {
                      price = cols(1).toDouble
                    }
                    case "TradeDiscount" => {
                      tradeDiscount = cols(1).toDouble
                    }
                    case "MRP" => {
                      mrp = cols(1).toDouble
                    }
                    case "Size" => {
                      size = cols(1)
                    }
                    case "CouponSaving" => {
                      couponSaving = cols(1).toDouble
                    }
                    case "PromotionSaving" => {
                      promotionSaving = cols(1).toDouble
                    }
                    case "Quantity" => {
                      qty = cols(1).toInt
                    }
                    case _ => 0
                }
            })
            order_case_class(row.getString(0), row.getString(1), productID, price, tradeDiscount, mrp, size, couponSaving, promotionSaving, qty)
          })
    }
    else{
      products = products :+ order_case_class(row.getString(0), row.getString(1), null, 0.0d, 0.0d, 0.0d, null, 0.0d, 0.0d, null)
    }
 
    return products
  }
  
   def convertOrderRowUsingMap(row: Row): Array[order_case_class] = {
    
    val productString = row.getAs[String]("ListOfProduct")
    var products = Array[order_case_class]()
    if(productString != null) {
          val items = productString.substring(2, productString.length() - 2).split("\\},\\{")
      
          products = items.map(item => {
            val product = item.split(",")

            var productMap = Map[String, String]()
            
            val productCol = product.map(p => {
              val cols = p.split("=")
              productMap = productMap + (cols(0)->cols(1))
            })

            order_case_class(row.getString(0), row.getString(1), 
                              productMap get "ProductId" get, productMap get "Price" map(_.toDouble) get, 
                              productMap get "TradeDiscount" map(_.toDouble) get, productMap get "MRP" map(_.toDouble) get, 
                              productMap get "Size" get, productMap get "CouponSaving" map(_.toDouble) get, 
                              productMap get "PromotionSaving" map(_.toDouble) get, productMap get "Quantity" map(_.toInt) get)
          })
    }
    else{
      products = products :+ order_case_class(row.getString(0), row.getString(1), null, 0.0d, 0.0d, 0.0d, null, 0.0d, 0.0d, null)
    }
 
    return products
  }
  
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\winutils");

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    import spark.implicits._

    val readFile = spark.read.json("./src/resources/exercise/ProductItemSample.json")

    readFile.printSchema

    val ordersDF = readFile.select($"SessionId", $"OrderId", $"ListOfProduct")
    ordersDF.show
    
    val resultDF = ordersDF.flatMap(convertOrderRow)
    resultDF.show
    
    val resultDFUsingMap = ordersDF.flatMap(convertOrderRowUsingMap)
    resultDFUsingMap.show

    println("Done")

    spark.stop
  }

}