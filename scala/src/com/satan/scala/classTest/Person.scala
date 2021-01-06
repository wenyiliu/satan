package com.satan.scala.classTest

/**
 * @author liuwenyi
 * @date 2020/12/14
 */
class Person {

  private var age = 18

  val range = 10

  lazy val array = new Array[Int](range)

  private var name: String = _

  def addAge(add: Int): Unit = {
    age += add
  }

  def defaultAge(): Unit = {
    age = 10
  }

  def getName: String = name

  def getAge: Int = {
    age
  }
}

