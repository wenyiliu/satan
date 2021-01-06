package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2021/01/04
 */
case class MaxMin(year: String, min: Int, max: Int)

object MaxMinSparkCore {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("max_min").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val maxMinRdd = sc.textFile("/Users/liuwenyi/IdeaProjects/satan/spark/src/main/scala/test.txt")
    maxMinRdd.map(line => (line.substring(8, 12), line.substring(line.length - 4).toInt))
      .groupByKey().map(item => {
      val itemList = item._2.toList
      MaxMin(item._1, itemList.min, itemList.max)
    }).foreach(println)
  }
}
