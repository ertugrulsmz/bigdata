package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object StreamFromKafka {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ReadFromKafka")
      .master("local[4]")
      .getOrCreate()

    // Kaynaktan okuma
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","deneme")
      .load()

    import spark.implicits._


    /*
+----+--------------------+------+---------+------+--------------------+-------------+
| key|               value| topic|partition|offset|           timestamp|timestampType|
+----+--------------------+------+---------+------+--------------------+-------------+
|null|[6D 65 72 68 61 6...|deneme|        0|    30|2019-05-17 23:53:...|            0|
+----+--------------------+------+---------+------+--------------------+-------------+
 */

    val df2 = df.select($"key".cast(StringType), $"value".cast(StringType))

/*
+----+-------------+
| key|        value|
+----+-------------+
|null|merhaba spark|
+----+-------------+

*/


    // Sonuç Çıktı
    val query = df2.writeStream
      .outputMode("append")
      .format("console")
      .start()


    query.awaitTermination()









  }

}
