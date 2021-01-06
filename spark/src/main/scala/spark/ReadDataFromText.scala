package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2020/12/16
 */
object ReadDataFromText {

  def main(args: Array[String]): Unit = {

    // 获取配置信息
    val conf = new SparkConf().setAppName("txt").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 从外部读取数据
    val rddData = sc.parallelize("/address/")

    // 读取第一行
    rddData.take(1)
  }
}
