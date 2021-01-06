package com.satan.scala.dataType

import scala.reflect.ClassManifestFactory.Nothing

/**
 * @author liuwenyi
 * @date 2020/12/27
 */
object DataTypeTest extends App {

  var a = 2
  var b: Short = 2
  val c = Nothing
  println(c)
  println(a == b)
  var d = 'a'
  var e = "a"
  println(d)
}
