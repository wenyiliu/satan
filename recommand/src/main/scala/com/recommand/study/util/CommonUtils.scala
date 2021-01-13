package com.recommand.study.util

import org.apache.spark.sql.{Column, DataFrame}

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
object CommonUtils {


  def copyDF(df: DataFrame): DataFrame = {
    val columnArr = df.columns.map(column => s"$column as ${column}Copy")
    // :_* 将一个数组传给可变参数函数
    df.selectExpr(columnArr: _*)
  }

  def concatCopy(column: String): String = {
    s"${column}Copy"
  }

  def join(leftDf: DataFrame, rightDf: DataFrame, onColumn: Array[String]): DataFrame = {
    var resultDf: DataFrame = null
    if (onColumn == null || onColumn.length == 0) {
      resultDf = leftDf.join(rightDf)
    } else {
      var condition: Column = null
      val leftDFColumnArr = leftDf.columns
      onColumn.foreach(column => {
        if (leftDFColumnArr.contains(column)) {
          // 拼接 on 的条件
          if (condition == null) {
            condition = leftDf.col(column) === rightDf.col(concatCopy(column))
          } else {
            condition.and(leftDf.col(column) === rightDf.col(concatCopy(column)))
          }
        } else {
          if (condition == null) {
            condition = rightDf.col(column) === leftDf.col(concatCopy(column))
          } else {
            condition.and(rightDf.col(column) === leftDf.col(concatCopy(column)))
          }
        }
      })
      resultDf = leftDf.join(rightDf, condition)
    }
    resultDf
  }

}

