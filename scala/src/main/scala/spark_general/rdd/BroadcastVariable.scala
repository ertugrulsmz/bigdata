package spark_general.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object BroadcastVariable {

    def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
      .set("spark.driver.memory","2g").setExecutorEnv("spark.executor.memory",value="3g")

    val sc = new SparkContext(sparkConf)

    def loadProduct():Map[Int,String]={
      var source = Source.fromFile("files/products.csv")
      var lines = source.getLines().filter(x=>(!x.contains("productCategoryId")))

      var productIdName: Map[Int, String] = Map()
      for(line <- lines){
        var productId = line.split(",")(0).toInt
        var productname = line.split(",")(2)

        productIdName += (productId -> productname)
      }

      return productIdName
    }

    // Demonstration of map acquintance
    var productMap = loadProduct()
    print(productMap.get(1))

    // Directly run the function
    var productBroadcast = sc.broadcast(loadProduct)
    println("\n Broadcast")
    print(productBroadcast.value(1))
    
    // get order items sc read auto rdd create
    val ordersRdd = sc.textFile("files/order_items.csv").filter(!_.contains("orderItemName"))

    def makeIdandTotal(line:String)={
      val orderItemProductId = line.split(",")(2).toInt
      val orderItemSubTotal = line.split(",")(4).toFloat

      (orderItemProductId,orderItemSubTotal)
    }
    
    var orderPairRdd = ordersRdd.map(makeIdandTotal)

    var reducedOrderPairRdd = orderPairRdd.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(ascending = false)
      .map(x=>(x._2,x._1))

    reducedOrderPairRdd.map(x=>(productBroadcast.value(x._1),x._2)).take(10).foreach(println)

  }

}
