package com.satan.scala.classTest

/**
 * @author liuwenyi
 * @date 2020/12/15
 */
object ListTest {

  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, -1, -2, 4) takeWhile (_ > 0)

    println(list)
  }

}
