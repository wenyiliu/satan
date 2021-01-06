package com.satan.scala

/**
 * @author liuwenyi
 * */
object Test extends App {

  val arr = Array(1, 2, 3, 4, 5, 6, 7)
  println(arr.mkString("Array(", ", ", ")"))
  arr.filter(x => x % 2 == 0).foreach(x => println(x))
  var array = Array("c", "a", "b", "a")
  println(array.map(a => (a, 1)).groupBy(_._1).mapValues(_.length))

}
