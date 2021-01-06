package sparkCore.userLog

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


/**
 * @author liuwenyi
 * @date 2021/01/04
 */
class ItemAcc extends AccumulatorV2[UserLogItem, mutable.Map[String, Long]] {

  item =>

  private val map: mutable.Map[String, Long] = mutable.Map[String, Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserLogItem, mutable.Map[String, Long]] = {
    val categoryAcc = new ItemAcc()
    map.synchronized(categoryAcc.map ++= map)
    categoryAcc
  }

  override def reset(): Unit = map.clear()

  override def add(v: UserLogItem): Unit = {
    val uid = v.guid
    v.target match {
      case 1 =>
        val key = uid + "_click"
        map += key -> (map.getOrElse(key, 0L) + 1L)
      case 0 =>
        val key = uid + "_nonClick"
        map += key -> (map.getOrElse(key, 0L) + 1L)
      case _ =>
    }
  }

  override def merge(other: AccumulatorV2[UserLogItem, mutable.Map[String, Long]]): Unit = {
    other match {
      case mergeItem: ItemAcc =>
        mergeItem.map.foreach {
          case (k, v) =>
            item.map += k -> (item.map.getOrElse(k, 0L) + v)
        }
        item.map ++= mergeItem.map.foldLeft(item.map) {
          case (map, (cidAction, count)) =>
            map += cidAction -> (map.getOrElse(cidAction, 0L) + count)
            map
        }
      case _ =>
    }
  }

  override def value: mutable.Map[String, Long] = map
}
