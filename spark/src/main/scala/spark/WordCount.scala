package spark

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2020/12/14
 */
object WordCount extends Logging {

  def main(args: Array[String]): Unit = {
    // 创建上下文
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)

    // 由现有集合创建 RDD,默认分区数为程序所分配到的 CPU 的核心数
    val dataRddDefault = sc.parallelize(data)

    // 计算分区数
    val partitions = dataRddDefault.getNumPartitions

    println(partitions)

    // 指定相应的分区
    val dataRdd = sc.parallelize(data, 3)
    dataRdd.persist()

    println(dataRdd.getNumPartitions)
    log.error("{}", dataRdd.getNumPartitions)
  }


}
