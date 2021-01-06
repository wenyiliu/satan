package sparkCore.userLog

import scala.collection.mutable

/**
 * @author liuwenyi
 * @date 2021/01/05
 */
case class UserLogStatisticResult(guid: String, click: Long, nonClick: Long, recentWatchMap: mutable.Map[Long, String], deviceTopNMap: mutable.Map[String, Long])

case class UserLogItem(id: Int, target: Int, timestamp: Long, deviceid: String,
                       newsid: String, guid: String, pos: Int, app_version: String, device_vendor: String,
                       netmodel: String, osversion: String, lng: String, lat: String, device_version: String, ts: Long)

case class UserLogResult(guid: String, clickRate: Double, recentWatch
: String, exposure: Long, maxUseDevice: String)