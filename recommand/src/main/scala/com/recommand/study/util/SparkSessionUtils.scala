package com.recommand.study.util

import org.apache.spark.sql.SparkSession

/**
 * @author liuwenyi
 * @date 2021/01/10
 */
object SparkSessionUtils {

  def getHiveSession(jobName: String, db: String): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(jobName)
      .enableHiveSupport()
      .getOrCreate()
    if (db != null || db != "") {
      spark.sql("use " + db)
    }
    spark
  }

  def getDefaultSession: SparkSession = {
    SparkSession.builder().master("local[*]")
      .appName("default")
      .getOrCreate()
  }
}
