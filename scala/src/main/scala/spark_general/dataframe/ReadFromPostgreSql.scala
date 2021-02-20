package spark_general.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadFromPostgreSql {
  def main(args:Array[String]):Unit = {

    /*
    <!-- https://mvnrepository.com/artifact/org.postgresql/postgresql -->
      <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>9.4.1207</version>
      </dependency>
    */

    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION  ************************
    val spark = SparkSession.builder()
      .appName("ReadFromPosgres")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://localhost:5432/spark2")
      .option("dbtable","simple_data")
      .option("user","postgres")
      .option("password","ertuqrul138")
      .load()

    df.show()



  }

}
