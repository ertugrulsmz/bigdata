package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


object DataCleaning {

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DataframeEntrance").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    import spark.implicits._

    val dirtyDF = spark.read.format("csv").
      option("header","true").option("sep",",").option("InferSchema","true")
      .load("files/simple_dirty_data.csv")

    //dirtyDF.show(5)

    val df2 = dirtyDF.withColumn("isim",trim(initcap($"isim")))
      .withColumn("cinsiyet",when($"cinsiyet".isNull,"U").otherwise($"cinsiyet"))
      .withColumn("sehir",when($"sehir".isNull,"bilinmiyor").otherwise($"sehir"))
      .withColumn("sehir",trim(upper($"sehir")))


    df2.coalesce(1).write.mode("Overwrite").option("sep",",").option("header","true")
      .csv("C:\\Users\\ertug\\Desktop\\305 analysis")
  }

}
