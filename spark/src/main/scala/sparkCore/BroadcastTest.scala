package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2021/01/06
 */
object BroadcastTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("broadcast_test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val v = Array(1, 2, 3, 4, 5)
    val bv = sc.broadcast(v)
    println(bv.value.mkString("Array(", ", ", ")"))
    bv.destroy()
  }
}
