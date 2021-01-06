package sparkCore.userLog

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author liuwenyi
 * @date 2021/01/05
 */
class UserLogAcc(val n: Int) extends AccumulatorV2[UserLogItem, mutable.Map[String, UserLogStatisticResult]] {

  private var map = mutable.Map[String, UserLogStatisticResult]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserLogItem, mutable.Map[String, UserLogStatisticResult]] = {
    val acc = new UserLogAcc(this.n)
    map.synchronized(acc.map ++ map)
    acc
  }

  override def reset(): Unit = map.clear()

  override def add(v: UserLogItem): Unit = {
    val user = v.guid
    val result = map.getOrElse(user, null)
    var click = 0L
    var nonClick = 0L
    v.target match {
      case 1 => click = if (result == null) 1L else 1L + result.click
      case 0 => nonClick = if (result == null) 1L else 1L + result.nonClick
      case _ =>
    }
    var recentWatchMap = if (result == null) mutable.Map[Long, String]() else result.recentWatchMap
    if (v.timestamp > 0) {
      recentWatchMap += v.timestamp -> v.deviceid
      if (recentWatchMap.size > n) {
        recentWatchMap -= recentWatchMap.min._1
      }
    }
    var deviceTopNMap = if (result == null) mutable.Map[String, Long]() else result.deviceTopNMap
    deviceTopNMap += v.device_version -> (deviceTopNMap.getOrElse(v.device_version, 0L) + 1L)
    map += user -> UserLogStatisticResult(user, click, nonClick, recentWatchMap, deviceTopNMap)
  }

  override def merge(other: AccumulatorV2[UserLogItem, mutable.Map[String, UserLogStatisticResult]]): Unit = {
    map = other match {
      case o: UserLogAcc =>
        map.foldLeft(o.map)((newMap, k) => {
          val user = newMap.getOrElse(k._1, null)
          val click = if (user == null) k._2.click else k._2.click + user.click
          val nonClick = if (user == null) k._2.nonClick else k._2.nonClick + user.nonClick
          val recentWatchMap: mutable.Map[Long, String] = if (user == null) k._2.recentWatchMap else {
            mutable.Map((k._2.recentWatchMap ++ user.recentWatchMap).toSeq.sortBy(_._1).take(n): _*)
          }
          val deviceTopNMap = if (user == null) k._2.deviceTopNMap else {
            k._2.deviceTopNMap.foldLeft(user.deviceTopNMap)((newDeviceTopNMap, k1) => {
              newDeviceTopNMap(k1._1) = newDeviceTopNMap.getOrElse(k1._1, 0L) + k1._2
              newDeviceTopNMap
            })
          }
          newMap(k._1) = UserLogStatisticResult(k._1, click, nonClick, recentWatchMap, deviceTopNMap)
          newMap
        })
    }
  }

  override def value: mutable.Map[String, UserLogStatisticResult] = map
}
