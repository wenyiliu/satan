package com.satan.scala.classTest

/**
 * @author liuwenyi
 * @date 2020/12/29
 */
class Student(val name: String, val age: Int) extends Person {

  def this(name: String, age: Int, birthday: String) {
    this(name, age)
    this.birthday = birthday
  }

  private[this] var birthday = "2020-10-10"

  def getBirthday: String = birthday

  override def toString: String = name + ":" + age + ":" + birthday
}

object Student {
  def main(args: Array[String]): Unit = {
    val student = new Student("xiaozhang", 10)
    println(student.age)
    println(student.getBirthday)
  }
}
