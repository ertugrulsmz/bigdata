package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions =>F}



object WriteToKafka {

  def main(Args:Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("WriteToKafka").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    import spark.implicits._

    val advertisementDF = spark.read.format("csv").option("header","true")
      .load("files/Advertising.csv")

    advertisementDF.show(4)

    // dollar sign equal to F.col()
    //advertisementDF.withColumn("key",$"ID").show(4)

    val df2 = advertisementDF.withColumn("key",F.col("ID"))
      .withColumn("value",F.concat(
        $"TV",F.lit(","),
        F.col("Radio"),F.lit(","),
        F.col("Newspaper"),F.lit(","),
        F.col("Sales")
      )).drop("ID","TV","Newspaper","Sales","Radio")


    df2.write.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","deneme").save()









  }

}
