package spark_general.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddTransformation {

  def printnumberwithSpace(x:Int):Unit = {
    print(x+" ")
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    //-----map
    val sc = spark.sparkContext
    val numbers = sc.makeRDD(List(1,2,3,4,5,6,7))
    numbers.map(x => x*x).take(5).foreach(print) //Array[int] type returned

    //----filter------
    println()
    numbers.filter(x => x>4).take(5).foreach(print)

    //------text -- parallelize
    println()
    val text = List("Come Home","I loved you once")
    val textrdd = sc.parallelize(text)
    textrdd.take(5).foreach(x=>{
      print(x)
      print(" ")
    })

    //map-flatmap
    println()
    textrdd.map(x=>x.toUpperCase()).take(3).foreach(x=>{print(x+" ")})
    println()
    textrdd.flatMap(x=>x.toUpperCase()).take(10).foreach(x=>{print(x+" ")})
    println()
    //flatmap flatten 1 level. When we done split it become 3
    textrdd.flatMap(x=>x.split(' ')).map(x=>x.toUpperCase()).take(10).foreach(x=>{print(x+" ")})
    println()
    textrdd.map(x=>x.split(' ')).take(10).foreach(x=> {print(x+" ")})

    //distinct
    println()
    val repititednumbers = sc.makeRDD(List(1,2,2,2,5,6,6,2,5))
    repititednumbers.distinct().take(5).foreach(print) //Array[int] type returned

    //sample
    println()
    repititednumbers.sample(true,0.1,46L).foreach(printnumberwithSpace)

    //Couple
    //union
    println()
    val n1 = sc.makeRDD(List(1,2,2,2,5,6,6,2,5))
    val n2 = sc.makeRDD(List(17,22,22,29,17,62,5,6))
    n1.union(n2).take(20).foreach(printnumberwithSpace)

    //intersection
    println()
    n1.intersection(n2).take(20).foreach(printnumberwithSpace)

    //subtract
    println()
    n2.subtract(n1).take(20).foreach(printnumberwithSpace)

    //cartesian
    println()
    n2.cartesian(n1).take(20).foreach(x=>print(x)) //implicitly foreach(print)

    //ACTIONS
    //collect
    println()
    n2.collect().foreach(print)

    //count
    println()
    var c = n2.count()
    print( c)

    //countbyvalue
    println()
    n2.countByValue().foreach(print)

    //takeOrdered sorted
    println()
    n2.takeOrdered(15).foreach(print)

    //reduce returned INT
    println()
    println(n2.reduce((x,y)=>x+y))

    println("Aggregation with tuple")
    print(n2.aggregate((0,0))(((x,y)=>(x._1+y,x._2+1)),((x,y)=>(x._1+y._1,x._2+y._2))))

    println("\n  agg without tuple")
    println(n2.aggregate(0)(((x,y)=>x+y),((x,y)=>x+y)))

    println("\n reduce for tuple agg")
    println(n2.map(x=>(x,1)).reduce((x,y)=>(x._1+y._1,x._2+y._2)))

    println("\n reduce for total")
    println(n2.reduce((x,y)=>x+y))

    println("\n agg by key")
    val pairs = sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))
    pairs.aggregateByKey(0)(((x,y)=>(x+y)),((x,y)=>(x+y))).foreach(println)

    println("\n reduce by key")
    pairs.reduceByKey((x,y)=>(x+y)).foreach(println)


    println("\n agg by key average")
    val pairs2x = pairs.mapValues(x=>(x,1))
    pairs2x.aggregateByKey((0,0))(
      (
      (x,y)=>(x._1+y._1,x._2+y._2)
      ),
      (
        (x,y)=>(x._1+y._1,x._2+y._2)
      )
    ).foreach(println)



    println("\n avg reduce by key")
    val pairs2 = pairs.mapValues(x=>(x,1))
    pairs2.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map{case (key,value)=>(key,value._1/value._2)}.foreach(println)

    println("\n instead of case multiple indexing with tuple of tuple")
    pairs2.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._1,x._2._1/x._2._2)).foreach(println)








  }

}
