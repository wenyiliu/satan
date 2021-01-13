package sparkCore.userLog

import org.apache.spark.util.AccumulatorV2


/**
 * @author liuwenyi
 * @date 2021/01/04
 */
class ItemAcc extends AccumulatorV2[Int, Int] {

  private var sum = 0

  // 判断是否是初始状态，sum 为 0 时表示累加器为初始状态
  override def isZero: Boolean = sum == 0

  // 执行器 executor 执行时需要拷贝累加器对象，把累加器对象序列化之后，从 driver 传到 executor
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new ItemAcc()
    acc
  }

  // 重置数据
  override def reset(): Unit = sum = 0

  // 累加数据
  override def add(v: Int): Unit = sum += v

  // 合并数据，所有的 executor 中累加器 value 合并
  override def merge(other: AccumulatorV2[Int, Int]): Unit = sum += other.value

  // 累加器的结果
  override def value: Int = sum
}
