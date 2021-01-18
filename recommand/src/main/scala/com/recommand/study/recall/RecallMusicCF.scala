package com.recommand.study.recall

import com.recommand.study.sql.HiveSql
import org.apache.spark.sql.SparkSession

/**
 * @author liuwenyi
 * @date 2021/01/13
 */
class RecallMusicCF(spark: SparkSession) extends BaseRecallCF {

  import spark.implicits._


  private var tableName = ""

  def this(spark: SparkSession, tableName: String) {
    this(spark)
    this.tableName = tableName
  }

  def getMusicCF(): Unit = {
    val userMusicFavoriteRate = getUserMusicFavoriteRate(spark)
    userMusicFavoriteRate.createOrReplaceTempView("user_music_favoriteRate_table")
    //    spark.sql("select * from user_music_favoriteRate_table").show(100)
    spark.sql(HiveSql.music_recall_molecular_sql).createOrReplaceTempView("music_2_music_molecular_table")
    //    spark.sql("select * from music_2_music_table").show(100)
    spark.sql(HiveSql.music_recall_denominator_sql).createOrReplaceTempView("music_2_music_denominator_table")

    spark.sql(HiveSql.music_recall_sim_sql).createOrReplaceTempView("music_2_music_sim_table")

    // 获取某个 musicId 相似 top 30
    spark.sql(HiveSql.music_recall_sim_top30_sql)
      .toDF()
      .drop("rank")
      .createOrReplaceTempView("music_2_music_sim_top30_table")

    // 获取某个 userId 最喜欢 top 20 的音乐
    spark.sql(HiveSql.user_favorite_rate_top20_sql)
      .toDF()
      .drop("rank")
      .createOrReplaceTempView("user_music_favorite_top20_table")

    val musicSimMap = spark.sql(HiveSql.music_sim_sql).rdd
      .map(x => (x(0).toString, x(1).toString))
      .collect()
      .toMap
    val musicAllSeq = musicSimMap.keys.toSeq
    val df = userMusicFavoriteRate.rdd
      .map(x => (x(0).toString, x(1).toString))
      .groupByKey()
      .map(x => {
        val diffSet = musicAllSeq diff x._2.toSeq
        val result = diffSet.map(y => (x._1, y, musicSimMap.getOrElse(y, "")))
          .filter(z => z._3 != "")
          .toArray
        result
      }).flatMap(m => m)
      .toDF("user_id", "music_id", "score")

    if (tableName == null || tableName == "") {
      df.show()
    } else {
      df.createOrReplaceTempView("music_recall_tmp_table")
      spark.sql(HiveSql.save_recall_table_sql(tableName, "music_recall_tmp_table"))
    }
  }
}
