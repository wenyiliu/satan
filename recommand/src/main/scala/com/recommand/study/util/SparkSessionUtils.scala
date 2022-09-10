package com.recommand.study.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author liuwenyi
 * @date 2021/01/10
 */
object SparkSessionUtils {

  def getHiveSession(jobName: String, db: String): SparkSession = {
    // master http://gcp-prod-bitmartetl-fuxi-chaincruiser.c.uniex-io.internal
    // "hive.metastore.uris", "thrift://gcp-prod-bitmartetl-cdh-001.c.uniex-io.internal:9083"
    System.setProperty("HADOOP_USER_NAME","jumpserver")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HiveSpark").set("hive.metastore.uris", "thrift://gcp-prod-bitmartetl-cdh-001.c.uniex-io.internal:9083")
//    val spark = SparkSession.builder.config("hive.metastore.uris", "thrift://gcp-prod-bitmartetl-cdh-001.c.uniex-io.internal:9083").appName("test").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
    //      .setAppName("test_001")
    //    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    if (db != null || db != "") {
      spark.sql("select * from db_levelage.dwb_al_order_borrow_add_1d WHERE ds = '2022-04-26' limit 10").show(10)
      println(spark.sql("show databases ").rdd.first().toString())
    }
    spark
  }

  def getDefaultSession: SparkSession = {
    System.setProperty("HADOOP_USER_NAME","jumpserver")
    val spark=SparkSession.builder.config("hive.metastore.uris","thrift://gcp-prod-bitmartetl-cdh-10-140-1-10.c.uniex-io.internal:9083").appName("test").enableHiveSupport().getOrCreate()
    spark.sql("select * from db_exchange.dm_entrustlog_chengjiao_2_hbase_dd where ds='2019-09-26' limit 10").show(10)
    SparkSession.builder().master("local[*]")
      .appName("default")
      .getOrCreate()
  }
}
