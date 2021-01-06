package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author liuwenyi
 * @date 2021/01/05
 */
object Test {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    //    val sc = new SparkContext(conf)
    //    val counter = sc.longAccumulator
    //    val data = Array(1, 2, 3, 4, 5)
    //    sc.parallelize(data).foreach(x => counter.add(x))
    //    println(counter.value)
    //
    var scores02 = mutable.Map("hadoop" -> 10,  "storm" -> 30)
    val scores03 = mutable.Map("hadoop" -> 10, "spark" -> 20, "storm" -> 1)
    val scores04 = Map(1 -> 10, 2 -> 20, 3 -> 30)
    println(scores03.max)
    for ((k, v) <- scores02) println(k + "====>" + v)
    scores02 = scores02.foldLeft(scores03)((map, t) => {
      map(t._1) = map.getOrElse(t._1, 0) + t._2
      map
    })
    for ((k, v) <- scores02) println(k + "*******" + v)
  }
}
