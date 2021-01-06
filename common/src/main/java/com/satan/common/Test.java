package com.satan.common;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2020/11/29
 */
public class Test {

    public static void main(String[] args) throws IOException {
//        String fileName = "/Users/liuwenyi/IdeaProjects/satan/common/src/main/java/com/satan/common/text.txt";
//        File file = new File(fileName);
//        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
//        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
//        List<String> list = new ArrayList<String>();
//        String line;
//        while ((line = bufferedReader.readLine()) != null) {
//            if (list.contains(line)) {
//                continue;
//            }
//            list.add(line);
//            System.out.println(line);
//        }

        String value = "394";
        double v = unitConversion(value);
        System.out.println(v);
    }

    private static List<Double> handleOutlier(List<Double> targetList) {
        if (targetList.size() == 1) {
            return targetList;
        }
        List<Double> restrictList = targetList;
        for (int i = 0; i < targetList.size() - 1; i++) {
            restrictList = doHandleOutlier(restrictList);
        }
        return restrictList;
    }

    private static List<Double> doHandleOutlier(List<Double> list) {
        if (list.size() == 1) {
            return list;
        }
        Double max = Collections.max(list);
        Double min = Collections.min(list);
        int multiple = BigDecimal.valueOf(max)
                .divide(BigDecimal.valueOf(min), 4, BigDecimal.ROUND_HALF_UP)
                .intValue();
        if (multiple < 10) {
            return list;
        }
        String multipleValue = String.valueOf(multiple);
        char ch = multipleValue.charAt(0);
        int len = multipleValue.length();
        StringBuilder sb = new StringBuilder();
        sb.append(ch);
        for (int i = 0; i < len - 1; i++) {
            sb.append("0");
        }
        double anInt = Double.parseDouble(sb.toString());
        List<Double> restrictList = Lists.newArrayList();
        for (Double item : list) {
            if (Objects.equals(item, max)) {
                Double restrictValue = BigDecimal.valueOf(max)
                        .divide(BigDecimal.valueOf(anInt), 3, BigDecimal.ROUND_HALF_UP)
                        .doubleValue();
                restrictList.add(restrictValue);
            } else {
                restrictList.add(item);
            }
        }
        return restrictList;
    }

    public static double unitConversion(String source) {
        BigDecimal newValue = BigDecimal.valueOf(Double.parseDouble(source));
        return newValue.divide(BigDecimal.valueOf(10), 4, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
