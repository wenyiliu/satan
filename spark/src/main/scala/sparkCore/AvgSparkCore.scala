package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2021/01/04
 */
object AvgSparkCore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("avg")
    val sc = new SparkContext(conf)
    val avgRdd = sc.textFile("/Users/liuwenyi/IdeaProjects/satan/spark/src/main/scala/test.txt")
    avgRdd.map(line => {
      val lines = line.split(" ")
      if (lines.length < 2) null else (lines(0), lines(1).toDouble)
    }).filter(item => item != null)
      .groupByKey()
      .map(item => {
        val itemList = item._2.toList
        val result: Double = itemList.sum / itemList.size.toDouble
        (item._1, result)
      }).foreach(println)
  }
}
