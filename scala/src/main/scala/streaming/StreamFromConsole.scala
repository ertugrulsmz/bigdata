package streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object StreamFromConsole {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ReadFromConsole")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Kaynaktan okuma
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()




    // Sonuç Çıktı
    val query = lines.writeStream
      .outputMode("append")
      .format("console")
      .start()


    query.awaitTermination()









  }

}
