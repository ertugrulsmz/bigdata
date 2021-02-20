package spark_general.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object JoinOrder {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","Join")

    val orderItemsRdd = sc.textFile("files/order_items.csv").filter(!_.contains("orderItemName"))
    orderItemsRdd.take(3).foreach(println)

    println("\n")
    val productRdd = sc.textFile("files/products.csv").filter(!_.contains("productDescription"))
    productRdd.take(3).foreach(println)

    //for join both rdd would be in format as (key,(value1,value2,value3..))
    def makeOrderItemsPairRDD(line:String) ={
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2)
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4)
      val orderItemProductPrice = line.split(",")(5)

      // orderItemProductId anahtar,  kalanlar değer olacak şekilde PairRDD döndürme
      (orderItemProductId, (orderItemName, orderItemOrderId, orderItemQuantity,orderItemSubTotal, orderItemProductPrice))
    }
    
    val orderItemsPairRDD = orderItemsRdd.map(makeOrderItemsPairRDD)
    print("\n")
    orderItemsPairRDD.take(3).foreach(println)

    def makeProductsPairRDD(line:String) ={
      val productId = line.split(",")(0)
      val productCategoryId = line.split(",")(1)
      val productName = line.split(",")(2)
      val productDescription = line.split(",")(3)
      val productPrice = line.split(",")(4)
      val productImage = line.split(",")(5)

      (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
    }

    val productPairRdd = productRdd.map(makeProductsPairRDD)
    
    println("\n")
    productPairRdd.take(3).foreach(println)
    
    
    val orderItemProductJoinRdd = orderItemsPairRDD.join(productPairRdd)
    print("\n join operation\n")
    orderItemProductJoinRdd.take(3).foreach(println)


    print("\n orderItem count "+orderItemsPairRDD.count()+" Product Count : "+productPairRdd.count()+" Joined count : "+orderItemProductJoinRdd.count())

  }

}
