package org.satan

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession


/**
 *
 * @author liuwenyi
 * @date 2022/4/19 
 * */
object ImportSessionAgr2Hbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("BulkloadTest")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    //从hive中读取数据,数据是在hdfs上，hive是个外部表，你也可以用内部表，都一样
    val dataFrame = spark.sql("select *,id rowkey from stu")

    val dataRdd = dataFrame.rdd.flatMap(row => { //cf是列族名,ID、DATA_TYPE、DEVICE_ID为字段名
      val rowkey = row.getAs[String]("rowkey".toLowerCase)
      Array(
        (rowkey, ("info", "id", row.getAs[String]("id"))),
        (rowkey, ("info", "name", row.getAs[String]("name")))
      )
    })
    //要保证行键，列族，列名的整体有序，必须先排序后处理,防止数据异常过滤rowkey
    val rdds = dataRdd
      .filter(x => x._1 != null)
      .sortBy(x => (x._1, x._2._1, x._2._2))
      .map(x => {
        //将rdd转换成HFile需要的格式,Hfile的key是ImmutableBytesWritable,那么我们定义的RDD也是要以ImmutableBytesWritable的实例为key
        //KeyValue的实例为value
        val rowKey = Bytes.toBytes(x._1)
        val family = Bytes.toBytes(x._2._1)
        val colum = Bytes.toBytes(x._2._2)
        val value = Bytes.toBytes(x._2._3)
        (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, value))
      })

    //临时文件保存位置，在hdfs上
    val tmpdir = "hdfs://node01:8020/tmp/test2"

    val hconf = new Configuration()
    hconf.set("fs.defaultFS", "hdfs://node01:8020")

    val fs = FileSystem.get(hconf)
    if (fs.exists(new Path(tmpdir))) {
      fs.delete(new Path(tmpdir), true)
    }

    //创建HBase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "10.130.0.9,10.130.0.8,10.130.0.10")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    //为了预防hfile文件数过多无法进行导入，设置该参数值
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 5000)
    //此处运行完成之后,在tmpdir生成的Hfile文件
    rdds.saveAsNewAPIHadoopFile(tmpdir,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)
    //开始HFile导入到Hbase
    val load = new LoadIncrementalHFiles(conf)
    //hbase的表名
    val tableName = "B_TEST_STU"

    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址
    val conn = ConnectionFactory.createConnection(conf)
    //根据表名获取表
    val table = conn.getTable(TableName.valueOf(tableName))

    try {

      //获取hbase表的region分布
      val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
      println("获取hbase表的region分布:   " + regionLocator)
      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(conf)

      //设置job名称，随便起一个就行
      job.setJobName("bulkload_stu_test")

      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])

      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      print("配置HFileOutputFormat2的信息-----开始导入----------")
      //配置HFileOutputFormat2的信息
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
      print("开始导入----------")
      //开始导入
      load.doBulkLoad(new Path(tmpdir), conn.getAdmin, table, regionLocator)
      print("结束导入----------")
    } finally {
      table.close()
      conn.close()
    }
  }
}
