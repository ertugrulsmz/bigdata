package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DirectSqlQuery {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DataframeEntrance").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    val dfFromFile = spark.read.format("csv")
      .option("sep", ";").option("header", "true").option("inferSchema", "true")
      .load("files/OnlineRetail.csv")

    dfFromFile.cache()

    dfFromFile.createOrReplaceTempView("onlineretail")

    val queriedDF = spark.sql(
      """
        SELECT Country,COUNT(Country) as counter
        from onlineretail
         group by Country
         order by counter desc
        """
    )

    //cache does begin working after this action.
    queriedDF.show(10)

  }

}
