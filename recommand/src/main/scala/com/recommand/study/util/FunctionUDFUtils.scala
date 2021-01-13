package com.recommand.study.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * @author liuwenyi
 * @date 2021/01/10
 */
object FunctionUDFUtils {


  val productUdf = udf((s1: Double, s2: Double) => s1 * s2)


  val filterUnListenUdf: UserDefinedFunction = udf { (items: Seq[String], items_v: Seq[String]) =>
    val fMap = items.map(x => {
      val l = x.split("_")
      (l(0), l(1))
    }).toMap
    items_v.filter(x => {
      val l = x.split("_")
      fMap.getOrElse(l(0), -1) == -1
    })
  }
  val simRatingUDF: UserDefinedFunction = udf { (sim: Double, items: Seq[String]) =>
    items.map { x =>
      val l = x.split("_")
      l(0) + "_" + l(1).toDouble * sim
    }
  }
}
