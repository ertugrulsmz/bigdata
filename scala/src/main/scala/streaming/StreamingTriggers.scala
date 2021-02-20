package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}

object StreamingTriggers {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val retailDataSchema = new StructType()
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", DateType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    val streamData = spark.readStream
      .schema(retailDataSchema)
      .option("maxFilesPerTrigger","2")
      .csv("C:\\Users\\ertug\\Desktop\\streamdict")

    val tumblingWindowAggregations = streamData
      .groupBy(
        window(col("InvoiceTimestamp"), "1 hours", "15 minutes"),
        col("Country")
      )
      .agg(sum(col("UnitPrice")))

    val sink = tumblingWindowAggregations
      .writeStream
      .trigger(Trigger.Once())
      .format("console")
      .option("truncate", "false")
      .outputMode("complete")
      .start()

    sink.awaitTermination()
  }
}