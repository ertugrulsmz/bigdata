package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.orderBy

object WordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DataframeEntrance").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    val storyDs = spark.read.textFile("files/omer_seyfettin_forsa_hikaye.txt")
    val storyDf = storyDs.toDF("column1")

    import org.apache.spark.sql.{functions =>F}
    //Explode example
    println("Explode instead of dataset, with dataframe")
    storyDf.withColumn("words",F.explode(F.split(F.col("column1")," "))).show(3)

    println("dataframe story")
    storyDf.show(5)

    print("dataset story")
    storyDs.show(5)

    import spark.implicits._

    val words = storyDs.flatMap(x=>x.split(" "))
    println("words count : ",words.count())
    words.show(5)

    // Aggregation and Grouping

    import org.apache.spark.sql.functions.{count}
    words.groupBy( $"value").agg(count($"value").as("wordCount"))
      .orderBy($"wordCount".desc).show(10)



    println("words in rdd format below. It needs to be paired and reduced by key ")
    words.rdd.take(10).foreach(print)

  }

}
