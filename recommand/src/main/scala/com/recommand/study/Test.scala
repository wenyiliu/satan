package com.recommand.study

import com.recommand.study.bean.ALSParam
import com.recommand.study.recall.RecallALS
import com.recommand.study.util.SparkSessionUtils


/**
 * @author liuwenyi
 * @date 2021/01/14
 */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionUtils.getHiveSession("als", "hive_test")
    val als = new RecallALS(spark, ALSParam(10, 0.01, 8, "uid", "iid", "rating"), "als_recall")
    als.aLSResult()
  }

  def test(): Unit = {
    val spark = SparkSessionUtils.getDefaultSession
    val arr = Array("a_1", "a_2", "b_1", "c_3")

    spark.createDataFrame(arr.map(x => {
      val xx = x.split("_")
      (xx(0), xx(1))
    })).toDF("name", "value").groupBy("name").count().show()

  }
}
