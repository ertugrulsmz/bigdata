package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Logger, Level}

object TumblingWindow {

  def main(args:Array[String]):Unit = {
    val spark  = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

    val retailDataSchema = new StructType()
      .add("id",IntegerType)
      .add("InvoiceNo", IntegerType)
      .add("StockCode", StringType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", DateType)
      .add("UnitPrice", DoubleType)
      .add("CustomerID", DoubleType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamingData = spark.readStream
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .schema(retailDataSchema)
      .load("C:\\Users\\ertug\\Desktop\\streamdict")

    //Windowing
    val groupedByWindowCountry = streamingData.groupBy(
      window(col("InvoiceTimestamp"),"1 hours").as("time"),
      col("Country")
    ).agg(sum(col("UnitPrice"))).sort(col("time").asc)

    val sink = groupedByWindowCountry.writeStream
      .format("console")
      .option("truncate","false")
      .outputMode("complete")
      .start()

    sink.awaitTermination()



}

}
