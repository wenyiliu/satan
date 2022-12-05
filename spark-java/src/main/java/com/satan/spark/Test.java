package com.satan.spark;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2022/11/17
 **/
@Slf4j
public class Test {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");

        String columns = "user_id,cid,device_id,ip_country,ip,local_lang,local_time_zone,imei,imsi,user_agent," +
                "emulator,root,proxy,app_lang,app_version,os_type,os_version,event_id";
        List<String> columnList = Lists.newArrayList(columns.split(","));
        JavaSparkContext context = new JavaSparkContext(conf);
        try {
            JavaRDD<String> records = context.textFile("file:////Users/liuwenyi/IdeaProjects/data-custom/data/part-0" +
                    "-0");
            records.flatMap((FlatMapFunction<String, String>) s -> {
                log.error(">>>>值：{}",s);
                JSONObject data = null;
                try {
                    data = JSONObject.parseObject(s.trim());
                } catch (Exception e) {
                    log.error(">>>>解析用户日志错误：{}", e.getMessage());
                }
                log.info("=======> {}", data);
                List<String> rowList = new ArrayList<>();
                if (Objects.nonNull(data)) {
                    JSONObject finalData = data;
                    columnList.forEach(item ->
                            log.info("当前字段 name=={},value=={}", item, finalData.get(item)));
                    rowList.add(columnList.stream()
                            .map(data::getString)
                            .collect(Collectors.joining("\u0001")));

                }
                return rowList.iterator();
            }).saveAsTextFile("file:////Users/liuwenyi/IdeaProjects/data-custom/data/out1");
        } catch (Exception e) {
            log.error(">>>>解析用户日志错误：{}", e.getMessage());
        } finally {
            log.info("Parsing Failed");
        }
    }
}
