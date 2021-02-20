package spark_general.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Coffeeshop {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
      .set("spark.driver.memory","2g").setExecutorEnv("spark.executor.memory",value="3g")

    val sc = new SparkContext(sparkConf)

    val retailRddwithHeader = sc.textFile("files/OnlineRetail.csv")
    retailRddwithHeader.take(3).foreach(println)

    //get first line turn into rdd and make subtraction as retail - firstrdd
    //retailRddwithHeader.filter(!_.contains("InvoiceNo")).take(3).foreach(println)

    println("Get rid of title")
    val retailRdd = retailRddwithHeader.mapPartitionsWithIndex(
      (id,iter) => if(id==0) iter.drop(1) else iter
    )
    
    println("Count : "+retailRdd.count())
    retailRdd.take(3).foreach(println)

    println("Unit amount smaller than 30 filter")
    retailRdd.filter(x=>x.split(";")(3).toInt <30).take(3).foreach(println)

    println("Coffee and Unit Price > 20")

    def coffePrice20(line:String):Boolean={
      var returnvalue = true
      var description = line.split(";")(2)
      var Unitprice = line.split(";")(5).trim().replace(",",".").toFloat

      returnvalue = description.contains("COFFEE") && Unitprice > 20.0
      returnvalue
    }

    retailRdd.filter(coffePrice20).take(3)
      .foreach(println)

    println("\n total price of canceled orders")
    val canceledPriceRdd = retailRdd.map(x=>{
      var isCancelled = if (x.split(";")(0).startsWith("C")) true else false

      var quantity = x.split(";")(3).toDouble
      var price = x.split(";")(5).replace(",",".").toDouble //that num includes ',' not '.'
      var totalprice = quantity*price
      (isCancelled,totalprice)
    })


    canceledPriceRdd.filter(x=>x._1).reduceByKey((x,y)=>x+y).
      map(x=>x._2).take(3).foreach(println)


    print("\n total price of canceled orders in indirect way")

    case class CanceledPrice(isCancelled: Boolean,totalprice: Double)
    val canceledPriceRdd2 = retailRdd.map(x=>{
      var isCancelled = if (x.split(";")(0).startsWith("C")) true else false

      var quantity = x.split(";")(3).toDouble
      var price = x.split(";")(5).replace(",",".").toDouble //that num includes ',' not '.'
      var totalprice = quantity*price
      CanceledPrice(isCancelled,totalprice)
    })

    // reduce by key require to be in tuple format not class obj
    canceledPriceRdd2.filter(x=>x.isCancelled).map(x=>(x.isCancelled,x.totalprice)).reduceByKey((x,y)=>x+y)
      .map(x=>x._2).take(3).foreach(println)
    //canceledPriceRdd.take(3).foreach(println)



  }



}
