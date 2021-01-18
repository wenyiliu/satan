package com.recommand.study

import com.recommand.study.bean.ALSParam
import com.recommand.study.recall.RecallALS
import com.recommand.study.util.SparkSessionUtils


/**
 * @author liuwenyi
 * @date 2021/01/14
 */
object ALSMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionUtils.getHiveSession("als", "hive_test")
    val als = new RecallALS(spark, ALSParam(10, 0.01, 8, "uid", "iid", "rating"), "als_recall")
    als.aLSResult()
  }
}
