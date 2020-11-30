package com.satan.hadoop.utils;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/19
 */
public class CFUtil {

    public static List<Integer> getItemList(FileSystem fs, String path) throws IOException {
        return HdfsUtil.getContextList(fs, path)
                .stream()
                .map(Integer::parseInt)
                .sorted()
                .collect(Collectors.toList());
    }

    public static List<String> getUserList(FileSystem fs, String path) throws IOException {
        return HdfsUtil.getContextList(fs, path)
                .stream()
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    public static Map<String, String> getUserMap(String path) throws IOException {
        FileSystem fs = FileSystem.get(HadoopConfiguration.getConfiguration());
        return HdfsUtil.getContextList(fs, path).stream()
                .collect(Collectors.toMap(line -> line.split(Constant.DEFAULT_SPLIT)[0],
                        line -> line.split(Constant.DEFAULT_SPLIT)[1]));

    }

    public static double similarUser(Map<String, String> map1, Map<String, String> map2) {
        if (map1 == null || map1.isEmpty() || map2 == null || map2.isEmpty()) {
            return Double.MIN_VALUE;
        }
        String[] s1 = new String[map1.size()];
        String[] s2 = new String[map2.size()];
        int index = -1;
        for (Map.Entry<String, String> entry : map1.entrySet()) {
            String value2 = map2.get(entry.getKey());
            if (Objects.isNull(value2)) {
                continue;
            }
            index++;
            s1[index] = entry.getValue();
            s2[index] = value2;
        }
        if (index == -1) {
            return Double.MIN_VALUE;
        }
        return DistanceUtil.euclideanDistance(s1, s2);
    }

    public static List<String> getTopNList(int topN, List<List<String>> list) {
        List<String> doubleList = list.stream()
                .flatMap(Collection::stream)
                .sorted((o1, o2) -> {
                    double d1 = Double.parseDouble(o1.split("\\|")[1]);
                    double d2 = Double.parseDouble(o2.split("\\|")[1]);
                    return Double.compare(d2, d1);
                })
                .collect(Collectors.toList());
        if (topN <= 0 || topN >= doubleList.size()) {
            return doubleList;
        }
        return doubleList.subList(0, topN);
    }

    public static Map<String, String> initMap(String targetUserItemScore) {
        String[] userItemScores = targetUserItemScore.split(Constant.COMMA);
        Map<String, String> map = new HashMap<>(userItemScores.length << 1);
        for (String itemScore : userItemScores) {
            String[] itemAndScore = itemScore.split("\\|");
            map.put(itemAndScore[0], itemAndScore[1]);
        }
        return map;
    }

//    public static void main(String[] args) {
//        List<String> list1 = Lists.newArrayList("1|1.1", "2|0.1");
//        List<String> list2 = Lists.newArrayList("1|2.1", "2|0.5");
//        List<List<String>> list = Lists.newArrayList(list1, list2);
//        List<String> topNList = getTopNList(9, list);
//        System.out.println(topNList);
//    }
}
