package com.satan.scala.classTest

import scala.collection.mutable.ArrayBuffer

/**
 * @author liuwenyi
 * @date 2020/12/15
 */
object ArrayTest {

  def main(args: Array[String]): Unit = {
    // 初始化数组
    val arr = Array("hello", "scala")

    // 定长数组
    val array = new Array[Int](10)

    // 变长数组
    val ab = new ArrayBuffer[Int]()

    ab += 1
    ab += (2, 3, 4)
    ab ++= Array(1, 2, 3)

    println(ab.toString())

    // 移除最后两个元素
    ab.trimEnd(2)
    ab.insert(1, 11, 13)
    ab.insertAll(5, Array(1111, 2222, 3333))
    println(ab.toString())
    for (index <- ab) {
      //  println(index)
    }

    val array1 = for (a <- ab) yield 10 * a
    println(array1)

    val array2 = ab.filter(_ % 2 == 0).map(_ * 10)
    println(array2)
  }
}
