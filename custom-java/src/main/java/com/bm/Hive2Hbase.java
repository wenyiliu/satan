package com.bm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2022/4/19
 **/
public class Hive2Hbase {

    private static final Logger LOG = LoggerFactory.getLogger(Hive2Hbase.class);

    private static final byte[] DEFAULT_CF = Bytes.toBytes("bm");

    public static class ORCMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put> {

        ImmutableBytesWritable ibw = new ImmutableBytesWritable();

        private String[] columns;

        private String[] rowkeys;

        @Override
        protected void setup(Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>.Context context) {
            Configuration configuration = context.getConfiguration();
            columns = configuration.get("columns", "").split(",");
            rowkeys = configuration.get("rowkeys", "").split(",");
        }

        @Override
        protected void map(NullWritable key, OrcStruct value, Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
            List<String> rowKeyList = Lists.newArrayList();
            TypeDescription schema = value.getSchema();
            List<String> fieldNameList = schema.getFieldNames();
            String[] lines = value.toString()
                    .replace("{", "")
                    .replace("}", "")
                    .split(", ");
            for (String columnName : rowkeys) {
                int index = fieldNameList.indexOf(columnName);
                rowKeyList.add(lines[index]);
            }
            String rk = String.join("_", rowKeyList);
            ibw.set(Bytes.toBytes(rk));
            Put put = new Put(Bytes.toBytes(rk));
            for (String column : columns) {
                int index = fieldNameList.indexOf(column);
                if (index < 0) {
                    LOG.error("index error {},columns={}", column, columns);
                    continue;
                }
                put.addColumn(DEFAULT_CF, Bytes.toBytes(column), Bytes.toBytes(lines[index]));
            }
            context.write(ibw, put);
        }
    }

    public static class TestMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        ImmutableBytesWritable ibw = new ImmutableBytesWritable();

        private List<String> columnList;

        private final Map<String, Integer> columnIndexMap = Maps.newHashMap();

        @Override
        protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) {
            Configuration configuration = context.getConfiguration();
            columnList = Arrays.asList(configuration.get("columns", "").split(","));
            // rowkey 字段名称
            String[] rowkeyColumnList = configuration.get("rowkeys", "").split(",");
            for (String column : rowkeyColumnList) {
                int i = columnList.indexOf(column);
                if (i <= 0) {
                    continue;
                }
                columnIndexMap.put(column, i);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //指定一下新表中的分隔符
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                LOG.error("该行数据为空，无法导入 HBASE");
                return;
            }
            String[] words = line.split("\\t");
            String rk = columnIndexMap.values()
                    .stream().map(index -> words[index])
                    .collect(Collectors.joining("_"));
            ibw.set(Bytes.toBytes(rk));
            Put put = new Put(Bytes.toBytes(rk));
            for (int i = 1; i < columnList.size(); i++) {
                put.addColumn(DEFAULT_CF, Bytes.toBytes(columnList.get(i)), Bytes.toBytes(words[i]));
            }
            context.write(ibw, put);
        }
    }

    public void run(String sorted, String zk, String tableName, String filePath, String columns, String rowkeys) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zk);
        config.set("mapreduce.job.queuename", "etl");
        config.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        config.set("columns", columns);
        config.set("rowkeys", rowkeys);
        config.set("dfs.client.socket-timeout", "180000");
        Job job = Job.getInstance(config, tableName);
        job.setJarByClass(Hive2Hbase.class);
        // 根据文件格式做校验
        if (Objects.equals("ORC", sorted)) {
            job.setInputFormatClass(OrcInputFormat.class);
            job.setMapperClass(ORCMapper.class);
        } else {
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(TestMapper.class);
        }
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        //设置一个reduce
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);
        TableMapReduceUtil.initTableReducerJob(tableName, null, job);

        FileInputFormat.setInputPaths(job, new Path(filePath));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
    }

    public static void main(String[] args) throws Exception {
        Hive2Hbase fileIntoHbase = new Hive2Hbase();
        fileIntoHbase.run(args[0], args[1], args[2], args[3], args[4], args[5]);
//        fileIntoHbase.run("ORC", "10.130.0.9,10.130.0.8,10.130.0.10", "dwd_al_account_detail_add_1d", "hdfs://NameNodeHACluster/data/warehouse/tablespace/external/hive/db_levelage.db/dwd_al_account_detail_add_1d/ds=2022-04-21", "id,account_id,symbol_id,symbol_name,coin_id,coin_name,order_id,order_type,amount,fund_type,remark,version,create_time,update_time,ds", "id");
    }
}
