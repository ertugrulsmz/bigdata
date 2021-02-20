package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataFrameEntrance {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DataframeEntrance").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dfFromList = sc.parallelize(List(1, 2, 3, 4, 5, 6, 5, 4)).toDF("numbers")
    dfFromList.printSchema()

    val dfFromSpark = spark.range(10, 100, 5).toDF("range")
    dfFromSpark.printSchema()

    val dsFromSpark = spark.range(10, 100, 5)
    dsFromSpark.printSchema()

    val dfFromFile = spark.read.format("csv")
      .option("sep", ";").option("header", "true").option("inferSchema", "true")
      .load("files/OnlineRetail.csv")

    dfFromFile.printSchema()
    //dfFromFile.show(10,truncate = false)

    println("retails DS")
    val retailDsspark = spark.read.textFile("files/OnlineRetail.csv")
    retailDsspark.show(5)


    println("Online retail count : " + dfFromFile.count)

    dfFromFile.select("InvoiceNo", "Quantity", "UnitPrice")
      .sort($"Quantity").show(10)

    spark.conf.set("spark.sql.shuffle.partitions", "4")

    dfFromFile.sort($"Quantity").explain()


  }

}
