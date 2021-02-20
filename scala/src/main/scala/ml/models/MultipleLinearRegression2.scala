package ml.models

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}


object MultipleLinearRegression2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    ////// SparkSession oluşturma
    val spark = SparkSession.builder
      .appName("CokluLineerRegresyonOdevCevabi")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()



    //  https://www.kaggle.com/kumarajarshi/life-expectancy-who
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("sep", ",")
      .load("files/LifeExpectancyData.csv")

    df.show(5)


    //Rename Columns instead of withColumnRenamed
    var newCols = Array("Country", "Year", "Status", "label", "AdultMortality",
      "InfantDeaths", "Alcohol", "PercentageExpenditure", "HepatitisB", "Measles", "BMI", "UnderFiveDeaths",
      "Polio", "TotalExpenditure", "Diphtheria", "HIV_AIDS", "GDP", "Population", "Thinness11", "Thinness59",
      "IncomeCompositionOfResources", "Schooling")

    val df2 = df.toDF(newCols:_*)

    df2.show(5)
    df2.printSchema()


    //Numerical cols are reduced by p value one by one in each iteration
    //That is optimum one
    var categoricCols = Array("Country","Status")
    var numericalCols = Array("Year", "AdultMortality",
      "InfantDeaths", "BMI", "UnderFiveDeaths",
      "TotalExpenditure",  "HIV_AIDS",
      "IncomeCompositionOfResources", "Schooling")


    var label = Array("label")

    //Drop null
    val df3 = df2.na.drop()

    //StringIndexer, Encoder
    val statusStringIndexer = new StringIndexer().setInputCol("Status").setOutputCol("StatusIdexed")
    val encoder = new OneHotEncoderEstimator().setInputCols(Array("StatusIdexed")).setOutputCols(Array("statusEncoded"))


    // VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(numericalCols ++ encoder.getOutputCols)
      .setOutputCol("features")


    //lineer model
    val linearRegressionObject = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    // pipeline model
    val pipelineObject = new Pipeline().setStages(Array(statusStringIndexer,encoder,vectorAssembler, linearRegressionObject ))


    // dataset split
    val Array(trainDF, testDF) = df3.randomSplit(Array(0.8, 0.2), 142L)
    trainDF.cache()
    testDF.cache()

    //Training
    val pipelineModel = pipelineObject.fit(trainDF)

    pipelineModel.transform(testDF).select("label","prediction").show()

    // pipeline model extract from pipeline
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

    // Model statistics
    // Regresyon modele ait  istatistikler
    println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
    println(s"R kare : ${lrModel.summary.r2}")
    println(s"Düzeltilmiş R kare : ${lrModel.summary.r2adj}")
    // Coeff
    println(s"Katsayılar : ${lrModel.coefficients}")
    println(s"Sabit : ${lrModel.intercept}")
    // p değerlerini görme. Not: Son değer sabit için
    println(s"p değerler: [${lrModel.summary.pValues.mkString(",")}]")
    // t değerlerini görme. Not: Son değer sabit için
    println(s"t değerler: [${lrModel.summary.tValues.mkString(",")}]")
    println("lrModel parametreleri: ")
    println(lrModel.explainParams)

    var pIcinNitelikler = numericalCols ++ Array("Status") ++ Array("sabit")
    var zippedPValues = pIcinNitelikler.zip(lrModel.summary.pValues)

    zippedPValues.map(x => (x._2, x._1)).sorted.foreach(println(_))

    /*

    RMSE: 3.5254437853102267
    R kare : 0.8402372243436562
    Düzeltilmiş R kare : 0.8377602820854183
    Katsayılar : [-0.12355922545254121,-0.016487905916958832,0.09207463315259633,-0.10151585599821678,1.6672627186749706E-4,-0.004862620120337882,-9.286073962789891E-6,0.02961449653259597,-0.0693630065752012,0.007929026904184913,0.09162580656087978,0.013548562915128126,-0.4457687844487546,3.8842013339422555E-5,-5.892479201299395E-11,0.03117283038069875,-0.07741593431832337,9.542424449579277,0.927515255589125,-0.8015887982878919]
    Sabit : 301.80667065140915
    p değerler: [2.214080842888322E-6,0.0,9.037215420448774E-14,0.0069944469886213945,0.4246739503761421,0.3280541792373892,0.46316882090430034,9.277282233099982E-6,1.021405182655144E-14,0.14696628529633227,0.041134342157394865,0.03194558950457549,0.0,0.2415689423147689,0.9774634776466289,0.6154505536890638,0.20657371462857865,0.0,0.0,0.033259070668361534,8.194222855806288E-9]
    t değerler: [-4.754380455278819,-15.586963645917912,7.536732417360849,-2.701440009581245,0.7985921274058335,-0.9784117241153895,-0.7338577032339971,4.451175533281614,-7.828215043967116,1.4512036605209422,2.044221640635109,2.147421825430189,-23.130498305606995,1.1716126773725288,-0.02825457834062044,0.5024303993946971,-1.2636786212598667,10.595421372670476,14.107831881001959,-2.1312268261197747,5.802870843016123]
    lrModel parametreleri:
    aggregationDepth: suggested depth for treeAggregate (>= 2) (default: 2)
    elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0)
    epsilon: The shape parameter to control the amount of robustness. Must be > 1.0. (default: 1.35)
    featuresCol: features column name (default: features, current: features)
    fitIntercept: whether to fit an intercept term (default: true)
    labelCol: label column name (default: label, current: label)
    loss: The loss function to be optimized. Supported options: squaredError, huber. (Default squaredError) (default: squaredError)
    maxIter: maximum number of iterations (>= 0) (default: 100)
    predictionCol: prediction column name (default: prediction)
    regParam: regularization parameter (>= 0) (default: 0.0)
    solver: The solver algorithm for optimization. Supported options: auto, normal, l-bfgs. (Default auto) (default: auto)
    standardization: whether to standardize the training features before fitting the model (default: true)
    tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-6)
    weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
    (0.0,AdultMortality)
    (0.0,HIV_AIDS)
    (0.0,IncomeCompositionOfResources)
    (0.0,Schooling)
    (1.021405182655144E-14,UnderFiveDeaths)
    (9.037215420448774E-14,InfantDeaths)
    (8.194222855806288E-9,sabit)
    (2.214080842888322E-6,Year)
    (9.277282233099982E-6,BMI)
    3. Tur (0.0069944469886213945,Alcohol)
    3. Tur (0.03194558950457549,Diphtheria)
    (0.033259070668361534,Status)
    (0.041134342157394865,TotalExpenditure)
    3. Tur  (0.14696628529633227,Polio)
    2. Tur (0.20657371462857865,Thinness59)
    2. Tur (0.2415689423147689,GDP)
    2. Tur (0.3280541792373892,HepatitisB)
    2. Tur (0.4246739503761421,PercentageExpenditure)
    2. Tur (0.46316882090430034,Measles)
    2. Tur (0.6154505536890638,Thinness11)
    1. Tur (0.9774634776466289,Population)



     */






  }


}
