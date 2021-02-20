package ml.models

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SimpleLinearRegression {

  def main(args: Array[String]):Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)



    //********* SPARK SESSION  ************************
    val spark = SparkSession.builder()
      .appName("LinearRegression")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    //********* Dataset Reading
    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("files/Advertising.csv")

    println("Initial Dataset")
    df.show()

    // Single Feature
    val df2 = df.withColumn("Advertisement", (col("TV") + col("Newspaper") + col("Radio")))
      .withColumnRenamed("Sales","label")
      .drop("TV","Radio","Newspaper")
    println( "df2 single feature: ")
    df2.show()


    //Describe
    println("Describe dataset")
    df2.describe("Advertisement", "label").show()


    //PreProcessing
    import  org.apache.spark.ml.feature.{VectorAssembler}
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("Advertisement"))
      .setOutputCol("features")


    // Regresssion model
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
    val lrObj = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Pipeline
    import org.apache.spark.ml.Pipeline
    val pipelineObj = new Pipeline()
      .setStages(Array(vectorAssembler, lrObj))

    // dataset split
    val Array(trainDF, testDF) = df2.randomSplit(Array(0.8, 0.2), 142L)

    //prediction
    val pipelineModel = pipelineObj.fit(trainDF)
    pipelineModel.stages.foreach(println(_))
    val resultDF = pipelineModel.transform(testDF)
    resultDF.show()

    //Fitted model
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

    println(s"R sq: ${lrModel.summary.r2}")
    println(s"Adjusted R sq: ${lrModel.summary.r2adj}")

    println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
    println(s"Coefficient: ${lrModel.coefficients}")
    println(s"Constant: ${lrModel.intercept}")
    // p değerlerini görme. Not: Son değer sabit için
    println(s"p values: [${lrModel.summary.pValues.mkString(",")}]")
    // t değerlerini görme. Not: Son değer sabit için
    println(s"t values: [${lrModel.summary.tValues.mkString(",")}]")

    println("Result df added residuals")
    resultDF.withColumn("residuals", (col("label") - col("prediction"))).show()

    // y = 4.537119328969264 + 0.04723638038563483 * Advertisement


    //Custom Dataset
    val predictDF = Seq(100).toDF("Advertisement")

    println("Different Predictions")
    val dfPredictionsVec = vectorAssembler.transform(predictDF)
    //Normally regressionobj.fit then it will create regresion model.
    //Since we used pipeline, we are retrieving fitted regressionModel
    lrModel.transform(dfPredictionsVec).show()


    println("--Fitted Pipeline Model--")
    pipelineModel.transform(predictDF).show()

    //Regression - Pipeline obj requires features with Model. But their model only desire features

  }

}
