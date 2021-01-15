package com.recommand.study

import com.recommand.study.recall.RecallMusicCF
import com.recommand.study.util.SparkSessionUtils
import org.apache.log4j.{Level, Logger}

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
object MusicRecallMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionUtils.getHiveSession("music_recall", "hive_test")
    Logger.getLogger("UserRecallMain").setLevel(Level.ERROR)
    var recallMusicCF: RecallMusicCF = null
    if (args.length == 0) {
      recallMusicCF = new RecallMusicCF(spark)
    } else {
      recallMusicCF = new RecallMusicCF(spark, args(0))
    }
    recallMusicCF.getMusicCF()
  }
}
