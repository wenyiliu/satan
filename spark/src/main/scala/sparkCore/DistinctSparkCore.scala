package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2021/01/04
 */
object DistinctSparkCore {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("distinct").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val distinctRdd = sc.textFile("")
    distinctRdd.distinct().saveAsTextFile("")
  }
}
