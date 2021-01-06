package sparkCore

import org.apache.spark.util.AccumulatorV2

/**
 * @author liuwenyi
 * @date 2021/01/05
 */
class CustomAcc extends AccumulatorV2[String, Int] {

  private var sum: Int = _

  override def isZero: Boolean = sum == 0

  override def copy(): AccumulatorV2[String, Int] = {
    val acc = new CustomAcc
    acc.sum = this.sum
    acc
  }

  override def reset(): Unit = sum = 0

  override def add(v: String): Unit = {
//    sum += v
  }

  override def merge(other: AccumulatorV2[String, Int]): Unit = {
    sum += other.value
  }

  override def value: Int = sum
}
