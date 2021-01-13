package com.recommand.study

import com.recommand.study.recall.RecallUserCF
import com.recommand.study.util.SparkSessionUtils
import org.apache.log4j.{Level, Logger}

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
object UserRecallMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionUtils.getHiveSession("user_recall", "hive_test")
    Logger.getLogger("org").setLevel(Level.ERROR)
    var recallUserCF: RecallUserCF = null
    if (args.length == 1) {
      recallUserCF = new RecallUserCF(spark, true)
    } else if (args.length >= 2) {
      recallUserCF = new RecallUserCF(spark, true, args(1))
    } else {
      recallUserCF = new RecallUserCF(spark)
    }
    recallUserCF.userItemRecall()
  }
}
