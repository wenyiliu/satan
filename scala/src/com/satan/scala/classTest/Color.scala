package com.satan.scala.classTest

/**
 * @author liuwenyi
 * @date 2020/12/14
 */
object Color extends Enumeration {

  type Color = Value

  val GREEN = Value
  val RED = Value(1)
  val BULE = Value(2, "蓝色")
}
