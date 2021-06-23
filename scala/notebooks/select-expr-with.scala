// Databricks notebook source
spark
/*
val spark = SparkSession.builder
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()
*/

// COMMAND ----------

// --------------------------------------------------------------CAST -----------------------------------------

// COMMAND ----------

val simpleData = Seq(("James",34,"true","M","3000.6089"), //seq is trait, default created one is list.
         ("Michael",33,"true","F","3300.8067"),
         ("Robert",37,"false","M","5000.5034")
     )
simpleData

// COMMAND ----------

import spark.implicits._
val df = simpleData.toDF("firstname","age","isGraduated","gender","salary")
df.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

val df2 = df.withColumn("salary2",col("salary").cast(IntegerType)) // If I said salary, it would owerrite salary column
//df.withColumn("salary",col("salary").cast("int"))
df2.printSchema()
df2.show()

// COMMAND ----------

df.select(col("salary").cast("int").as("salary")).printSchema()

df.select(col("salary").cast("int").as("salary")).show()

// COMMAND ----------

df.selectExpr("cast(salary as int ) as salary_ ").printSchema()
df.selectExpr("cast(salary as int ) as salary_ ").show()

// COMMAND ----------

df.createOrReplaceTempView("CastExample")
val df4=spark.sql("SELECT firstname,age,isGraduated,INT(salary) as salary from CastExample")
df.show(1)

// COMMAND ----------


