package ml.preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Preprocess1 {

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

    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("files/simple_data.csv")

    println("Orjinal DF:")
    df.show()

    //prepare Label
    val df1 = df.withColumn("ekonomik_durum",
      when(col("aylik_gelir").gt(7000),"iyi")
        .otherwise("kötü")
    )
    println("ekonomical added DF:")
    df1.show()


    val meslekIndexer = new StringIndexer()
      .setInputCol("meslek")
      .setOutputCol("meslekIndex")
      .setHandleInvalid("skip")

    val meslekIndexerModel = meslekIndexer.fit(df1)
    val meslekIndexedDF = meslekIndexerModel.transform(df1)

    println("meslek column StringIndex added DF: meslekIndexedDF")
    meslekIndexedDF.show()


    println("Mesleklerin frekansları: ")
    df.groupBy(col("meslek"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("meslek"))
      .show()


    val sehirIndexer = new StringIndexer()
      .setInputCol("sehir")
      .setOutputCol("sehirIndex")
      .setHandleInvalid("skip")


    val sehirIndexerIndexModel = sehirIndexer.fit(meslekIndexedDF)
    val sehirIndexedDF = sehirIndexerIndexModel.transform(meslekIndexedDF)

    println("sehir sütunu için StringIndex eklenmiş DF: sehirIndexedDF")
    sehirIndexedDF.show()

    println("Şehirlerin frekansları: ")
    df.groupBy(col("sehir"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("sehir"))
      .show()


    // OneHot Encoder Stage
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array[String]("meslekIndex","sehirIndex"))
      .setOutputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded"))

    val encoderModel = encoder.fit(sehirIndexedDF)
    val oneHotEncodeDF = encoderModel.transform(sehirIndexedDF)

    println("oneHotEncodeDF:")
    oneHotEncodeDF.show()


    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded","yas","aylik_gelir"))
      .setOutputCol("vectorizedFeatures")

    val vectorAssembledDF = vectorAssembler.transform(oneHotEncodeDF)


    println("vectorAssembledDF: ")
    vectorAssembledDF.show(false)

    //LabelIndexer------
    val labelIndexer = new StringIndexer()
      .setInputCol("ekonomik_durum")
      .setOutputCol("label")

    val labelIndexerModel = labelIndexer.fit(vectorAssembledDF)
    val labelIndexerDF = labelIndexerModel.transform(vectorAssembledDF)

    println("labelIndexerDF: ")
    labelIndexerDF.show(truncate = false)

    //Standart Scaler
    val scaler = new StandardScaler()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("features")

    val scalerModel = scaler.fit(labelIndexerDF)
    val scalerDF = scalerModel.transform(labelIndexerDF)

    println("scalerDF: ")
    scalerDF.show(truncate = false)

    //Train-Test Split -- Looks like python tuple yet different in return value
    val Array(trainDF, testDF) =  scalerDF.randomSplit(Array(0.8, 0.2), 142L)

    println("trainDF: ")
    trainDF.show(false)
    println("testDF: ")
    testDF.show(false)


    import org.apache.spark.ml.classification.LogisticRegression

    val lrObj = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val lrModel = lrObj.fit(trainDF)

    println("ML test result DF: ")
    lrModel.transform(testDF).show()

    println("Ml train result DF")
    lrModel.transform(trainDF).show(truncate=false)








  }

}
