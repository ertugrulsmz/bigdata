package ml.preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression

object Pipeline1 {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    ///////////////////// SPARK SESSION OLUŞTURMA /////////////////////
    val spark = SparkSession.builder()
      .appName("StringIndexerOps")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext

    ///////////////////// VERİ OKUMA ///////////////////////////////////
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("files/simple_data.csv")

    println("Orjinal DF:")
    df.show()

    val df1 = df.withColumn("ekonomik_durum",
      when(col("aylik_gelir").gt(7000),"iyi")
        .otherwise("kötü")
    )

    val Array(trainDF, testDF) = df1.randomSplit(Array(0.8, 0.2), 142L)

    val meslekIndexer = new StringIndexer()
      .setInputCol("meslek")
      .setOutputCol("meslekIndex")
      .setHandleInvalid("skip")

    val sehirIndexer = new StringIndexer()
      .setInputCol("sehir")
      .setOutputCol("sehirIndex")
      .setHandleInvalid("skip")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array[String]("meslekIndex","sehirIndex"))
      .setOutputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded"))

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded","yas","aylik_gelir"))
      .setOutputCol("vectorizedFeatures")

    val labelIndexer = new StringIndexer()
      .setInputCol("ekonomik_durum")
      .setOutputCol("label")

    val scaler = new StandardScaler()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("features")

    val classifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val pipeline = new Pipeline()
    pipeline.setStages(Array(meslekIndexer,sehirIndexer,encoder,vectorAssembler,labelIndexer,scaler,classifier))

    val pipelineModel = pipeline.fit(trainDF)

    //Transform returns dataframe
    pipelineModel.transform(testDF).show(false)





  }



}
