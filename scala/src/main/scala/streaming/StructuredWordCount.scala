package streaming

import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object StructuredWordCount {

  def main(args:Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ReadFromConsole")
      .master("local[4]")
      .getOrCreate()

    // Read From File
    val lines = spark.readStream
      .format("text")
      .load("C:\\Users\\ertug\\Desktop\\streamdict")

    import spark.implicits._

    //operatıon and transformation----------

    //implicit dataframe to dataset
    val words = lines.as[String].flatMap(_.split(" "))

    val wordcount = words.groupBy("value").agg(F.count(F.col("value")).as("counter"))
      .sort(F.desc("counter"))


    // Streming Start //agg complete mode, normal df append
    val query = wordcount.writeStream.outputMode("complete")
      .format("console").start()

    //dataset writer type query
    query.awaitTermination()









  }

}
