package com.satan.hadoop.utils;

import com.google.common.collect.Lists;
import com.satan.hadoop.config.HadoopConfiguration;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/19
 */
public class HdfsUtil {

    public static void deletePath(String path) throws IOException {
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

    public static List<String> getContextList(FileSystem fs, String path) throws IOException {
        Path inPath = new Path(path);
        if (!fs.exists(inPath)) {
            throw new IOException("the cluster center path is not exist:" + path);
        }

        // 读取文件
        FileStatus[] fileStatuses = fs.listStatus(inPath);

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
        }).filter(Objects::nonNull).flatMap(s -> s).collect(Collectors.toList());
    }

    public static void appendToFile(String contentPath, String appendFilePath) throws IOException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        Path path = new Path(appendFilePath);
        if (!fs.exists(path)) {
            fs.create(path);
        }
        FSDataOutputStream out = fs.append(path);
        FileStatus[] fileStatuses = fs.listStatus(new Path(contentPath));
        for (FileStatus f : fileStatuses) {
            FSDataInputStream in = fs.open(f.getPath());
            IOUtils.copyBytes(in, out, 4096);
        }
//        IOUtils.closeStream(out);

    }

    public static void appendContentToFile(FileSystem fs, String content, String appendFilePath) throws IOException {
        Path path = new Path(appendFilePath);
        if (!fs.exists(path)) {
            fs.create(path);
        }
        byte[] bytes = content.getBytes();
        FSDataOutputStream append = fs.append(path);
        append.write(bytes);
        append.close();
    }
}
