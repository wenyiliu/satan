package com.satan.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2022/4/22
 **/
public class ORCSample {

    public static class ORCMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {
        @Override
        protected void map(NullWritable key, OrcStruct value, Mapper<NullWritable, OrcStruct, Text, Text>.Context context) throws IOException, InterruptedException {
            System.out.println(value.getFieldValue(1));
        }
    }

//    public static class ORCReducer extends Reducer<Text, Text, NullWritable, OrcStruct> {
//        private TypeDescription schema = TypeDescription
//                .fromString("struct<name:string,mobile:string>");
//        private OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
//
//        private final NullWritable nw = NullWritable.get();
//
//        public void reduce(Text key, Iterable<Text> values, Context output)
//                throws IOException, InterruptedException {
//            for (Text val : values) {
//                pair.setFieldValue(0, key);
//                pair.setFieldValue(1, val);
//                output.write(nw, pair);
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("orc.mapred.output.schema", "struct<name:string,mobile:string>");
        Job job = Job.getInstance(conf, "ORC Test");
        job.setJarByClass(ORCSample.class);
        job.setMapperClass(ORCMapper.class);
//        job.setReducerClass(ORCReducer.class);
        job.setInputFormatClass(OrcInputFormat.class);
//        job.setOutputFormatClass(OrcOutputFormat.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(OrcStruct.class);
        FileInputFormat.addInputPath(job, new Path("/user/hive/warehouse/ods_country_code"));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
