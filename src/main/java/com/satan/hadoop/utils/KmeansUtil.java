package com.satan.hadoop.utils;

import com.google.common.collect.Lists;
import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
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
        Path clusterCenterPath = new Path(path);
        if (!fs.exists(clusterCenterPath)) {
            throw new IOException("the cluster center path is not exist:" + path);
        }

        // 读取文件
        FileStatus[] fileStatuses = fs.listStatus(clusterCenterPath);

        // 将聚类中心转成 map 形式。key 为聚类中心，value 为聚类中心坐标
        return Lists.newArrayList(fileStatuses).stream().map(item -> {
            FSDataInputStream open;
            try {
                open = fs.open(item.getPath());
            } catch (IOException e) {
                return null;
            }
            InputStreamReader reader = new InputStreamReader(open);
            BufferedReader bufferedReader = new BufferedReader(reader);
            return bufferedReader.lines();
        }).filter(Objects::nonNull).flatMap(s -> s)
                .collect(Collectors.toMap(line -> line.split(Constant.DEFAULT_SPLIT)[0],
                        line -> {
                            String cluster = line.split(Constant.DEFAULT_SPLIT)[1];
                            if (StringUtils.isBlank(cluster)) {
                                return new String[]{};
                            }
                            return cluster.split(Constant.COMMA);
                        }));
    }

    public static void deleteLastResult(String path) throws IOException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        Path inPath = new Path(path);
        fs.delete(inPath, true);
    }

    public static void copy(String sourcePath, String targetPath) throws IOException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path source = new Path(sourcePath);
        if (!fileSystem.exists(source)) {
            throw new IOException(sourcePath + " is not exist");
        }
        Path target = new Path(targetPath);
        if (fileSystem.exists(target)) {
            fileSystem.delete(target, true);
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(source);
        FSDataOutputStream outputStream = fileSystem.create(target);
        for (FileStatus status : fileStatuses) {
            FSDataInputStream in = fileSystem.open(status.getPath());
            IOUtils.copyBytes(in, outputStream, 4096);
        }
        IOUtils.closeStream(outputStream);
//        deleteLastResult(sourcePath);
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
            deleteLastResult(oldPath);
            return true;
        }
        copy(newPath, oldPath);
        return false;
    }
}
