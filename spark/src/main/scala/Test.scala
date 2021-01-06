

/**
 * @author liuwenyi
 * @date 2020/11/24
 */
object Test extends App {

  val arr = Array(1, 2, 3, 4)
  val v = for (a <- arr.indices) yield arr(a) * 10
  println(v)

}
