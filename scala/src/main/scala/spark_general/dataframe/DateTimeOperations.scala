package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions =>F}

object DateTimeOperations {

  def main(Args:Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DataframeEntrance").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    import spark.implicits._

    val dateDF = spark.read.format("csv").
      option("header","true").option("sep",";").option("InferSchema","true")
      .load("files/OnlineRetail.csv").select("InvoiceDate").distinct()

    println("Original date")
    dateDF.show(5)

    val formatOfInvoiceDate = "dd.MM.yyyy HH:mm"
    val desiredFormatforTR = "dd/MM/yyyy HH:mm:ss"
    val desiredFormatforEng = "MM-dd-yyyy HH:mm:ss"

    val df2 = dateDF.withColumn("InvoiceDate",F.trim($"InvoiceDate"))
      .withColumn("NormalData",F.to_date($"InvoiceDate",formatOfInvoiceDate))
      .withColumn("StandartTs",F.to_timestamp($"InvoiceDate",formatOfInvoiceDate))
      .withColumn("TrDate",F.date_format($"StandartTs",desiredFormatforTR))
      .withColumn("EngDate",F.date_format($"StandartTs",desiredFormatforEng))
      .withColumn("OneyearLater",F.date_add($"StandartTs",365))
      .withColumn("differences",F.datediff($"OneyearLater",$"StandartTs"))

    df2.show(5)


  }

}
