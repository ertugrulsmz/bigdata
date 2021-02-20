package ml.preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object DataCleaning {

  def main(args: Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataExplore")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val adultTrainDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("files/adult.data")


    val adultTestDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("files/adult.test")



    val adultWholeDF = adultTrainDF.union(adultTestDF)

    // !!!----NORMALLY COUNT ON DATAFRAME RETURNS JUST NUMBER BUT AFTER GROUP BY IT WORKS LIKE AGGREGATION
    // --SUM AUTOMATICALLY SUM THE ALL QUANTIATIVE COLUMNS ON GROUP
    //groupby count.
    //adultWholeDF.groupBy("workclass").agg(F.count($"*")).show(5)
    //adultWholeDF.groupBy("workclass").count().show(5)
    adultWholeDF.groupBy("workclass").sum().show(5)


    //Trimming
    val adultWholeDF1 = adultWholeDF
      .withColumn("workclass", F.trim(F.col("workclass")))
      .withColumn("education", F.trim(F.col("education")))
      .withColumn("marital_status", F.trim(F.col("marital_status")))
      .withColumn("occupation", F.trim(F.col("occupation")))
      .withColumn("relationship", F.trim(F.col("relationship")))
      .withColumn("race", F.trim(F.col("race")))
      .withColumn("sex", F.trim(F.col("sex")))
      .withColumn("native_country", F.trim(F.col("native_country")))
      .withColumn("output", F.trim(F.col("output")))


    val adultWholeDF2 = adultWholeDF1.withColumn("output",F.regexp_replace($"output","<=50K.","<=50K"))
      .withColumn("output",F.regexp_replace($"output",">50K.",">50K"))

    println("output groupby see if 2 group left")
    adultWholeDF2.groupBy($"output")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)
    // class count to 2.




    //Check if ? includes

    var columnswithQMark:Seq[String] = Seq()
    print("If columns include ? \n")
    for(column <- adultWholeDF2.columns){
      if(adultWholeDF2.filter(F.col(column).contains("?")).count() > 0){
        println(column + " includes ? ------- ")
        columnswithQMark = columnswithQMark:+column
      }else{
        println(column + "Does not includes  ")
      }
    }

    println("Seqcolumns with ? "+columnswithQMark)

    columnswithQMark = columnswithQMark:+"output"

    //See Relationship between marks and emptiness
    adultWholeDF2.select("workclass","occupation","native_country","output")
      .filter(
        F.col("workclass").contains("?") ||
          F.col("occupation").contains("?") ||
          F.col("native_country").contains("?")
      )
      .groupBy("workclass","occupation","native_country","output").count()
      .orderBy($"count".desc).show(50)


    //Same can be done for null controll traverse every column filter is null
    /*
    for(column <- adultWholeDF2.columns){
      if(adultWholeDF2.filter(F.col(column).isNull).count() > 0){
        println(column + " includes null ------- ")
        columnswithQMark = columnswithQMark:+column
      }else{
        println(column + "Does not includes  ")
      }
    }*/


    //Remove Question Marked Rows ------------------------

    val adultWholeDF3 = adultWholeDF2.filter(!(
      F.col("workclass").contains("?") ||
        F.col("occupation").contains("?") ||
        F.col("native_country").contains("?")
    ))

    println("After Cleaning ? \n")
    println("question marked df count : "+adultWholeDF2.count())
    println("question mark cleaned df count : "+adultWholeDF3.count())


    //Remove Weak Clases--------------------------
    // In work class never worked-without pay. Occupation Armed forces

    val adultWholeDF4 = adultWholeDF3.filter(!(
      F.col("workclass").contains("never-worked") ||
        F.col("workclass").contains("without-pay") ||
        F.col("occupation").contains("Armed-Forces") ||
        F.col("native_country").contains("Holand-Netherlands")
      ))

    println("After Removing Weak Classes \n")
    println("question marked df count : "+adultWholeDF3.count())
    println("question mark cleaned df count : "+adultWholeDF4.count())


    //Merging Education Category -----------
    /*
    1st-4th, 5th-6th, 7th-8th: elementary-school
    9th, 10th, 11th, 12th: high-school
    Masters, Doctorate: high-education
    Bachelors, Some-college: undergraduate
  */

    val adultWholeDF5 = adultWholeDF4.withColumn("education_merged",
      F.when($"education".isin("1st-4th","5th-6th","7th-8th"),"Elementary-School")
        .when($"education".isin("9th", "10th", "11th", "12th"),"High-School")
        .when($"education".isin("Masters","Doctorate"),"Post-Graduate")
        .otherwise($"education")
    )


    // After Merging
    println("education merged")
    adultWholeDF5.groupBy($"education_merged")
      .agg(F.count($"*").as("count"))
      .show(false)


    adultWholeDF5.show(5)



    val qualifiedSorting = Array[String]("workclass", "education", "education_merged", "marital_status", "occupation", "relationship", "race",
      "sex", "native_country", "age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week","output")

    val adultWholeDF6 = adultWholeDF5.select(qualifiedSorting.head, qualifiedSorting.tail:_*)
    adultWholeDF6.show(5)

    // Write to the Disk

    adultWholeDF6
      .coalesce(1)
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("C:\\Users\\ertug\\Desktop\\programming\\redundant\\cleaned_adult")
    // Yazdığımız yerde dosyayı Notepad++ ile kontrol edelim
    // Nitelik sırası düzgün ve satır sayısı da 45.208 (başlık dahil) sorun yoktur.




  }

}
