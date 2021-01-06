package com.satan.scala.classTest

/**
 * @author liuwenyi
 * @date 2020/12/29
 */
class Employee extends Person {

  override val range = 9

}

object Employee {
  def main(args: Array[String]): Unit = {
    val emp = new Employee()
    println(emp.array.mkString("(", ",", ")"))
  }
}
