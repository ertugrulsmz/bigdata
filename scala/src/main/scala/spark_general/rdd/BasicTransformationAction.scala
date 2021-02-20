package spark_general.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
RDD can work with foreach
 */

object BasicTransformationAction {

  def printnumberwithSpace(x:Int):Unit = {
    print(x+" ")
  }

  def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
      .set("spark.driver.memory","2g").setExecutorEnv("spark.executor.memory",value="3g")

    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.parallelize(List(3,22,3,5,22,7,8,11))
    rdd1.foreach(printnumberwithSpace)

    println()
    val rdd2 = sc.makeRDD(List("Spark learning is easy task"))
    rdd2.map(x=>x.toUpperCase).foreach(print)
    print("\n count : "+rdd2.count())

    println()
    val rdd3 = sc.parallelize(List(3,22,3,5,22,7,8,11,3))
    rdd3.map(x=>(x,1)).reduceByKey((x,y)=>x+y).filter(x=>x._1>5).foreach(println)


  }


}
