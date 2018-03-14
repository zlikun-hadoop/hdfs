package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * HDFS文件、目录操作API测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-14 13:25
 */
public class FileSystemTest {

    @Test
    public void test() throws IOException {

        // 设置用户名 ( 解决权限问题 )
        System.setProperty("HADOOP_USER_NAME", "root");

        // 使用默认配置获取文件系统对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // 创建目录
        // drwxr-xr--   - root supergroup          0 2018-03-14 15:16 /zlikun
        createDir(fs, "/zlikun");

        // 创建文件
        // -rw-r--r--   3 root supergroup         18 2018-03-14 15:16 /zlikun/server.txt
        create(fs, "/zlikun/server.txt", "nginx tomcat jetty");

        // 追加内容
        // 需要配置：dfs.support.append = true
        append(fs, "/zlikun/server.txt", " undertow");

        // 获取文件
        list(fs, "/zlikun");

        // 获取文件( 打印文件内容 )
        get(fs, "/zlikun/server.txt");

        // 删除文件
        delete(fs, "/zlikun/server.txt");

        // 判断目录是否存在
        existsDir(fs, "/zlikun");

        // 删除目录
        deleteDir(fs, "/zlikun");

    }

    /**
     * 查询文件
     * @param fs
     * @param path
     * @throws IOException
     */
    private void get(FileSystem fs, String path) throws IOException {
        Path path0 = new Path(path);
        assertEquals("MD5-of-0MD5-of-512CRC32C:3cbd70bca222bef122d500cd27097fb0", fs.getFileChecksum(path0).toString());
        FSDataInputStream in = fs.open(path0);
        // nginx tomcat jetty undertow
        IOUtils.copyBytes(in, System.err, 4096, false);
    }

    /**
     * 删除文件
     * @param fs
     * @param path
     */
    private void delete(FileSystem fs, String path) throws IOException {
        assertTrue(fs.deleteOnExit(new Path(path)));
    }

    /**
     * 遍历文件
     * @param fs
     * @param path
     */
    private void list(FileSystem fs, String path) throws IOException {
        FileStatus[] list = fs.listStatus(new Path(path));
        Arrays.stream(list).forEach(status -> {
            // 打印文件路径
            System.err.println(status.getPath().toString());
        });
    }

    /**
     * 创建文件并写入内容
     * @param fs
     * @param path
     * @param content
     * @throws IOException
     */
    private void create(FileSystem fs, String path, String content) throws IOException {
        InputStream in = new ByteArrayInputStream(content.getBytes());
        FSDataOutputStream out = fs.create(new Path(path), true);
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 追加文件内容
     * @param fs
     * @param path
     * @param content
     */
    private void append(FileSystem fs, String path, String content) throws IOException {
        InputStream in = new ByteArrayInputStream(content.getBytes());
        FSDataOutputStream out = fs.append(new Path(path));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 创建目录
     * @param fs
     * @param path
     */
    private void createDir(FileSystem fs, String path) throws IOException {

//        assertTrue(fs.mkdirs(new Path(path)));
        // 指定文件系统权限参数
        assertTrue(fs.mkdirs(new Path(path), new FsPermission(
                FsAction.ALL,   // user
                FsAction.ALL,   // group
                FsAction.READ   // other
        )));

    }

    /**
     * 判断目录是否存在
     * @param fs
     * @param path
     */
    private void existsDir(FileSystem fs, String path) throws IOException {
        assertTrue(fs.exists(new Path(path)));
    }

    /**
     * 删除目录
     * @param fs
     * @param path
     */
    private void deleteDir(FileSystem fs, String path) throws IOException {
        assertTrue(fs.deleteOnExit(new Path(path)));
    }

}
