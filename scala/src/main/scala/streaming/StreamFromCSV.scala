package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType,DateType}

object StreamFromCSV {


    def main(args: Array[String]): Unit = {
      val spark  = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

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

      val q = streamingData.writeStream.format("console").outputMode("append")
        .start()

      q.awaitTermination()

    }


}
