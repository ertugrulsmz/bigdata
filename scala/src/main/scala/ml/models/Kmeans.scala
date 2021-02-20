package ml.models

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Kmeans {
  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark session
    val spark = SparkSession.builder()
      .appName("KMeansMallCustomerBasic")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // veri okuma
    val df1 = spark.read.format("csv")
      .option("header",true)
      .option("sep",",")
      .option("inferSchema",true)
      .load("files/Mall_Customers.csv")

    // df.show()

    df1.printSchema()

    df1.describe().show()

    val newColumns = Array("CustomerID","Gender","Age","AnnualIncome","SpendingScore")
    val df = df1.toDF(newColumns:_*)


    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("AnnualIncome", "SpendingScore"))
      .setOutputCol("features")


    val standardScaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    def runKMeans(df:DataFrame, k:Int):PipelineModel= {
      val kMeansObject = new KMeans()
        .setFeaturesCol("scaledFeatures")
        .setSeed(142)
        .setPredictionCol("cluster")
        .setK(k)
      val pipelineObj = new Pipeline()
        .setStages(Array(vectorAssembler, standardScaler, kMeansObject))

      val pipelineModel = pipelineObj.fit(df)

      pipelineModel
    }





      for (k <- 2 to 11){
        val pipelineModel = runKMeans(df,k)
        val transformedDF2 = pipelineModel.transform(df)
        val evaluator = new ClusteringEvaluator()
          .setFeaturesCol("scaledFeatures")
          .setPredictionCol("cluster")
          .setMetricName("silhouette")
        val score = evaluator.evaluate(transformedDF2)
        println(k, score)
      }


    val pipelineModel = runKMeans(df, 5)
    val transformedDFFinal= pipelineModel.transform(df)

    transformedDFFinal.show()







  }





}
