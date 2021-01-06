package sparkCore.userLog

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2021/01/04
 */
object UserLog {

  def line2UserLogItem(line: String): UserLogItem = {
    val lineList = line.split(",")
    UserLogItem(
      lineList(0).toInt,
      lineList(1).toInt,
      if (lineList(2) == "NULL") 0L else lineList(2).toLong,
      lineList(3),
      lineList(4),
      lineList(5),
      lineList(6).toInt,
      lineList(7),
      lineList(8),
      lineList(9),
      lineList(10),
      lineList(11),
      lineList(12),
      lineList(12),
      lineList(14).toLong)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("user_log")
    val sc = new SparkContext(conf)
    val userLogRdd = sc.textFile("hdfs://hadoop01:9000/user/root/spark_core/user_log.csv")
    val userLogItemRdd = userLogRdd.mapPartitionsWithIndex((index, iterator) => {
      if (index == 0) iterator.drop(1) else iterator
    }).map(line => line2UserLogItem(line))
    val acc = new UserLogAcc(10)
    sc.register(acc)
    userLogItemRdd.foreach(action => acc.add(action))
    sc.parallelize(acc.value.values.toSeq)
      .map(f = item => {
        val exposure = item.click + item.nonClick
        val clickRate: Double = item.click / exposure.toDouble
        val recentWatch = item.recentWatchMap.values.take(10).toArray.mkString("(",",",")")
        val maxUseDevice = item.deviceTopNMap.toSeq.sortBy(_._2).take(1).head._1
        UserLogResult(item.guid, clickRate, recentWatch, exposure, maxUseDevice)
      }).saveAsTextFile("hdfs://hadoop01:9000/user/root/spark_core/user_log_result")
  }
}
