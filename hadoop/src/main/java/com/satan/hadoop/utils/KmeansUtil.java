package com.satan.hadoop.utils;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/17
 */
public class KmeansUtil {

    /**
     * 读取聚类中心  1\t2.1,1.3,0.8
     *
     * @return Map
     * @throws IOException IOException
     */
    public static Map<String, String[]> getClusterCenterMap(FileSystem fs, String path) throws IOException {
        return HdfsUtil.getContextList(fs, path).stream()
                .collect(Collectors.toMap(line -> line.split(Constant.DEFAULT_SPLIT)[0],
                        line -> {
                            String cluster = line.split(Constant.DEFAULT_SPLIT)[1];
                            if (StringUtils.isBlank(cluster)) {
                                return new String[]{};
                            }
                            return cluster.split(Constant.COMMA);
                        }));
    }


    public static boolean isFinished(String oldPath, String newPath, double threshold) throws IOException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        // 获取中心距离
        Map<String, String[]> oldCenters = getClusterCenterMap(fs, oldPath);
        Map<String, String[]> newCenters = getClusterCenterMap(fs, newPath);
        Set<String> set = oldCenters.keySet();
        double loss = 0.0;
        for (String cluster : set) {
            String[] oldCenter = oldCenters.get(cluster);
            String[] newCenter = newCenters.get(cluster);
            loss += DistanceUtil.lossFunction(oldCenter, newCenter);
        }
        if (loss < threshold) {
            HdfsUtil.deletePath(oldPath);
            return true;
        }
        HdfsUtil.copy(newPath, oldPath);
        return false;
    }
}
