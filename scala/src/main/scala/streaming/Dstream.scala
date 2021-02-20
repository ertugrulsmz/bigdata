package streaming

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}

object Dstream {

  def main(args:Array[String]):Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkContext

    val sc = new SparkContext("local[4]","WindowOps")


    //StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //ssc.checkpoint("D:\\checkpoint")

    // DStream from file
    val lines = ssc.textFileStream("C:\\Users\\ertug\\Desktop\\streamdict")

    //-----OPERATION-----------------------------------------------------

    val words = lines.flatMap(_.split(" "))

    val mappedWords = words.map(x => (x,1))

    val window = mappedWords.window(Seconds(30), Seconds(10)).reduceByKey(_+_)
      .transform(_.sortByKey(false))


    // 1 window()

    // val window = mappedWords.window(Seconds(30), Seconds(10))


    // 2. countByWindow()

    //val countByWindow = mappedWords.countByWindow(Seconds(30), Seconds(10))



    //---OPERATION END------
    window.print()

    //Listen and perform
    ssc.start()


    ssc.awaitTermination()











  }

}
