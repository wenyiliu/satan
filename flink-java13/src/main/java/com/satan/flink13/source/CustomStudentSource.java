package com.satan.flink13.source;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.satan.flink13.entrty.Student;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Random;

/**
 * @author liuwenyi
 * @date 2022/11/30
 **/
public class CustomStudentSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    private List<String> nameList = Lists.newArrayList("梁元帝", "高僧", "吴六奇", "梅念笙", "戚长发",
            "连城", "水笙", "狄云", "花铁", "喇嘛", "天山童姥", "段正淳", "凌千里", "萧笃诚", "董思归", "朱丹臣", "李秋水",
            "丁春秋", "乔峰", "王语嫣", "段誉", "慕容复", "鸠摩智", "曲灵风", "杨过", "杨康", "秦南琴", "郭靖", "穆念兹",
            "欧阳锋", "裘千仞", "欧阳克", "黄裳", "秦南琴", "穆念兹", "梅超风", "马钰", "韦小宝", "康熙");

    private List<String> addressList = Lists.newArrayList("山西省", "辽宁省", "吉林省", "黑龙江省",
            "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省", "河南省", "湖北省", "湖南省", "广东省", "海南省",
            "四川省", "贵州省", "云南省", "陕西省", "甘肃省", "青海省", "台湾省");


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Random random = new Random();
        int id = 1;
        while (isRunning) {
            long timeMillis = System.currentTimeMillis();
            long l = timeMillis - random.nextInt(10000);
            Student student = new Student(id, random.nextInt(20),
                    nameList.get(random.nextInt(39)), addressList.get(random.nextInt(22)), l);
            String value = JSON.toJSONString(student);
            ctx.collect(value);
            id++;
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {

    }
}
