package com.bm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapreduce.OrcInputFormat;

/**
 * @author liuwenyi
 * @date 2022/4/24
 **/
public class BulkLoadRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "10.130.0.9,10.130.0.8,10.130.0.10");
        config.set("mapreduce.job.queuename", "etl");
        config.set(TableOutputFormat.OUTPUT_TABLE, "dwd_al_account_detail_add_1d");
        config.set("columns", "id,account_id,symbol_id,symbol_name,coin_id,coin_name,order_id,order_type,amount,fund_type,remark,version,create_time,update_time,ds");
        config.set("rowkeys", "id");
        config.set("dfs.client.socket-timeout", "180000");
        int run = ToolRunner.run(config, new BulkLoadRunner(), args);
        // "ORC", "10.130.0.9,10.130.0.8,10.130.0.10", "dwd_al_account_detail_add_1d", "hdfs://NameNodeHACluster/data/warehouse/tablespace/external/hive/db_levelage.db/dwd_al_account_detail_add_1d/ds=2022-04-21", "id,account_id,symbol_id,symbol_name,coin_id,coin_name,order_id,order_type,amount,fund_type,remark,version,create_time,update_time,ds", "id"
        System.exit(run);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "dwd_al_account_detail_add_1d");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://NameNodeHACluster/data/warehouse/tablespace/external/hive/db_levelage.db/dwd_al_account_detail_add_1d/ds=2022-04-21"));
        job.setInputFormatClass(OrcInputFormat.class);
        job.setMapperClass(Hive2Hbase.ORCMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //将输出格式设置为Hfile
        job.setOutputFormatClass(HFileOutputFormat2.class);
        String outputPath = "hdfs://10.130.0.7:8020/hbase/dwd_al_account_detail_add_1d/" + System.currentTimeMillis();
        Path output = new Path(outputPath);
        //设置输出Hfile的目录
        HFileOutputFormat2.setOutputPath(job, output);
        //连接到Hbase
        Connection connection = ConnectionFactory.createConnection(conf);
        //获取要写入数据获的表
        Table table = connection.getTable(TableName.valueOf("dwd_al_account_detail_add_1d"));
        //通过HFileOutputFormat2将job和Hbase表关联
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf("dwd_al_account_detail_add_1d")));
        boolean b = job.waitForCompletion(true);
        if (b) {
            Admin admin = connection.getAdmin();
            LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
            load.doBulkLoad(output, admin, table, connection.getRegionLocator(TableName.valueOf("dwd_al_account_detail_add_1d")));
        }
        return b ? 0 : 1;
    }
}
