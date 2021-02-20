package ml.preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object DataExplore {

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

    println("\n adultTrainDF")
    adultTrainDF.show(5)


    println("\n adultTestDF")
    //adultTestDF.show(5)


    val adultWholeDF = adultTrainDF.union(adultTestDF)
    adultWholeDF.show(5)


    //Checking count to confirm
    /*
     println("adultTrainDF satır sayısı: ")
     println(adultTrainDF.count())
     println("adultTestDF satır sayısı: ")
     println(adultTestDF.count())
     println("adultWholeDF satır sayısı: ")
     println(adultWholeDF.count())
    */

    println("adultWholeDF schema: ")
    adultWholeDF.printSchema()

    println("Feature Analysis")

    // ---Workclass---------
    adultWholeDF.groupBy($"workclass").agg(F.count($"*").as("counter")).show(5)

    /*
            * Yorum: 2.799 adet ? var. Bu nedir. Muhtelemen kayıp bilgi.
            * Daha sonra never-worked ve without-pay sınıfları çok az tekrarlanmış. Bunların da veri setinden
            * çıkarılması düşünebilir.
     */

    // 2. =====================  education  =================================
    println("education groupby")
    adultWholeDF.groupBy($"education")
      .agg(F.count($"*").as("counter"))

      .show()
    /*
    Yorum: Genel bir sıkıntı görünmüyor ancak çok fazla kategori var belki bazıları birleştirilebilir.
              1st-4th, 5th-6th, 7th-8th: elementary-school
              9th, 10th, 11th, 12th: high-school
              Masters, Doctorate: high-education
              Bachelors, Some-college: undergraduate
     */

    // 3. =====================  marital_status  =================================

    println("marital_status groupby ")
    adultWholeDF.groupBy($"marital_status")
      .agg(F.count($"*").as("counter"))
      .show(false)
    //Yorum: Sorun görünmüyor.

    // 4. =====================  occupation  =================================

    println("occupation groupby ")
    adultWholeDF.groupBy($"occupation")
      .agg(F.count($"*").as("counter"))
      //.count()
      .show(false)

    /*
     Yorum: 2809 tane ? var. Bunlar muhtemelen bilinmeyenler. Ayrıca Armed-Forces 15 kişi.
              Bu sınıfa ait kayıtlar çıkarılabilir.
     */
    // 5. =====================  relationship  =================================

    println("relationship groupby ")
    adultWholeDF.groupBy($"relationship")
      .agg(F.count($"*").as("counter"))
      //.count()
      .show(false)

    // Yorum: Sorun yok

    // 6. =====================  race  =================================

    println("race groupby ")
    adultWholeDF.groupBy($"race")
      .agg(F.count($"*").as("counter"))
      //.count()
      .show(false)

    //* Yorum: Sorun görünmüyor. Çoğunluk white.
    // 7. =====================  sex  =================================

    println("sex groupby ")
    adultWholeDF.groupBy($"sex")
      .agg(F.count($"*").as("counter"))
      //.count()
      .show(false)
    // Yorum: Üçte biri kadın kalanı erkek


    // 8. =====================  native_country  =================================

    println("native_country groupby ")
    adultWholeDF.groupBy($"native_country")
      .agg(F.count($"*").as("counter"))
      //.count()
      .show(false)
    // Yorum: Büyük çoğunluk USA'den

    // 9. =====================  output  =================================

    println("output groupby ")
    adultWholeDF.groupBy($"output")
      .agg(F.count($"*").as("counter"))
      //.count()
      .show(false)
    // Yorum: "." içeren sonuçlar var. Bunların temizlenmesi gerekir.


    /////////////////////// VERİ TEMİZLİĞİ İÇİN TAVSİYELER  ////////////////////////////////////////////////

    /*
      1. Tüm sütunları boşluk kontrolü yap.
      2. ? içeren workclass, occupation  var bunların ? içerdiği satırlar tekrar incelenmeli.
          ? işaretleri sistematik bir şekilde mi oluşmuş yoksa bu oluşum tesadüfi mi?
          ? kayıtlarının oluşması altında yatan bir mekanizma var mı?
          Bu sistematik hata yakalanırsa veri doldurma (imputation) yoksa satır silme yapılsın.
      3. workclass niteliğinde never-worked ve without-pay sınıfları ve
          occupation niteliğinde  Armed-Forces  sınıfı
        çok az tekrarlanmış. Veri setinden çıkarılabilir.
      4. education niteliğindeki:
              1st-4th, 5th-6th, 7th-8th: elementary-school
              9th, 10th, 11th, 12th: high-school
              Masters, Doctorate: high-education
              Bachelors, Some-college: undergraduate
         sınıfları yukarıdaki gibi birleştirilebilir.
      5. native_country'de ? var ve Hollanda 1 kez tekrarlanmış.
      6. output (hedef değişkendeki) >50K. ve <=50K. değerlerindeki "." kaldırılmalı
     */

  }

}
