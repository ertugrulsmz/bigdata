package spark_general.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object DataSetApi {

  case class Company(name: String, foundingYear: Int, numEmployees: Int)
  case class Person(name: String, age: Int)

  case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
  case class Department(id: Int, name: String)

  case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
  case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("DataframeEntrance").master("local[4]")
      .config("spark.driver.memory", "2g").config("spark.executor.memory", "4gb")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dfFromList = sc.parallelize(List(1, 2, 3, 4, 5, 6, 5, 4)).toDF("numbers")
    dfFromList.show()

    val dfFromSpark = spark.range(10, 100, 5).toDF("range")
    dfFromSpark.printSchema()

    //---------------DATASET-------------------------
    println("Dataset")
    val dataset = Seq(1, 2, 3).toDS()
    dataset.show()

    println("Dataframe")
    val df1 = Seq(1, 2, 3).toDF("num")
    df1.show()

    println("Case seq to DS")

    val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
    personDS.show()
    personDS.collect().foreach(x=>println("x.name : "+x.name+ " type : "+x.name.getClass))


    //RDD -----------------------> DF - DS
    //Typed
    println("Rdd to DS")
    val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
    val integerDS = rdd.toDS()
    integerDS.show()
    integerDS.collect().foreach(x=>println("x but x._1 can be used dataset : "+x+ " type : "+x._1.getClass+" type : "+x._2.getClass))

    //Untyped
    println("Rdd to DF")
    val rdd2 = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
    val integerDF = rdd2.toDF("number","string")
    integerDF.show()
    integerDF.collect().foreach(x=>println("x : "+x+ " type : "+x.getClass))


    println("companyy rdd to df")
    val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
    val dfseq = sc.parallelize(inputSeq).toDF()
    dfseq.show()
    dfseq.collect().foreach(x=>println("x : "+x+ " type : "+x.getClass))

    //DF TO DS
    println("DF TO DS conversion")
    val companyDS = dfseq.as[Company]
    companyDS.show()
    companyDS.collect().foreach(x=>println("x : "+x+ " type : "+x.getClass))

    //WORDCOUNT DATASET
    println("word count ds")
    val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
    val wordssplitDS = wordsDataset.flatMap(_.toLowerCase.split(" "))
    println("splitted ds")
    wordssplitDS.show()

     val countsDataset = wordssplitDS.filter(_ != "")
      .groupBy("value").count()
    countsDataset.show()

    //WORDCOUNT DF
    println("word count df")
    val wordsDF = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDF()
    wordsDF.withColumn("value",
    F.explode(F.split($"value"," "))).show()


    //DATASET UNION
    println("Dataset Average Employee")

    val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
    val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
    val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

    val employeeDataset = employeeDataSet1.union(employeeDataSet2)

    def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
      val (total, count) = iterator.foldLeft(0.0, 0.0) {
        case ((total, count), x) => (total + x.salary, count + 1)
      }
      ResultSet(key._1, key._2, total/count)
    }


    //in join it still used x._1.name where refers first table
    val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
      .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
      .filter(record => record.age > 25)
      .groupBy($"departmentId", $"departmentName")
      .avg()

    averageSalaryDataset.show()

    //Observation
    println("2 normal and inner joined dataset")

    employeeDataset.show(5)
    departmentDataSet.show(5)

    val joinDs= employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
    joinDs.show(5,truncate=false)

    print("Ugly as a dataframe")
    joinDs.toDF().show(5,truncate=false)

    println("map this ugly looking dataset")
    val joinDsmapped = joinDs.map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
    joinDsmapped.show(5,truncate=false)

    println("mapped whis ugky looking dataset with tuple")
    val joindsMappedwithTuple = joinDs.map(x=>(x._1.name,x._1.salary,x._1.age,x._2.name))
    joindsMappedwithTuple.show(5,truncate=false)

    println("change columns name")
    joindsMappedwithTuple.withColumnRenamed("_1","first").show(5)


    //-------------DATAFRAME JOIN
    println("SAME sets and join with dataframe")
    val employmentDF = employeeDataset.toDF()
    val departmentsDF = departmentDataSet.toDF()

    println("Dataframe joinned")
    val joinned = employmentDF.join(departmentsDF,$"departmentId" === $"id")
    joinned.show(5,truncate=false)

    joinned.groupBy("departmentId","id")
      .avg().show(5,truncate=false)

    println("Map function on dataframe require explicit type convert and makes Ds from df\n")
    val joinedds = joinned.map(x=>(x(0).asInstanceOf[String],x(3).asInstanceOf[Double]))
    val joinedds2 = joinedds.withColumnRenamed("_1","name").
      withColumnRenamed("_2","salary")
    joinedds2.show(false)
    joinedds2.groupBy("name").agg(F.avg($"salary")).show(5)



    println("Read RDD from text file and convert dataset")
    // If no case class created it is mandotary to make :

    /*
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))
  */
    // And create schemaType for that.
    /*
      val peopleDF = spark.createDataFrame(rowRDD, schema)
    */



    // --------- Create an RDD of Person objects from a text file, convert it to a Dataframe
    /*
    val peopleDF = spark.sparkContext
      .textFile("examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    */


    print("User defined Functions")

    val columns = Seq("Seqno","Quote")
    val data = Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy.")
    )
    val df = data.toDF(columns:_*)
    df.show(5,truncate=false)

    //Udf function first letter to capital letter
    val convertCase =  (strQuote:String) => {
      val arr = strQuote.split(" ")
      val casedArr = arr.map(f=>  f.substring(0,1).toUpperCase + f.substring(1,f.length))
      casedArr.mkString(" ") //like python array join
    }

    //Register to Dataframe
    import org.apache.spark.sql.functions.udf
    val convertUDF = udf(convertCase)

    df.select(F.col("Seqno"),
      convertUDF(F.col("Quote")).as("Quote") ).show(false)


    // Using it on SQL
    spark.udf.register("convertUDF", convertCase)
    df.createOrReplaceTempView("QUOTE_TABLE")
    spark.sql("select Seqno, convertUDF(Quote) as q from QUOTE_TABLE").show(false)

    //Array
    println("Array on Dataframe")
    val df2 = df.withColumn("array",F.split(F.col("Quote")," "))
    df2.show(false)

    df2.printSchema()












  }

}
