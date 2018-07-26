package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-14 15:40
 */
public class LocalFileSystemTest {

    @Test
    public void test() throws IOException {

        // 设置用户名 ( 解决权限问题 )
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration conf = new Configuration();

        // 读取HDFS文件
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path("/txt/lang.txt"));

        // 写入到本地( Windows )
        LocalFileSystem lfs = FileSystem.getLocal(conf);
        FSDataOutputStream out = lfs.create(new Path("/local_lang.txt"));

        // 复制文件
//        byte [] buf = new byte[16];
//        int length = 0;
//        while ((length = in.read(buf)) != -1) {
//            out.write(buf, 0, length);
//            out.flush();
//        }
//        out.close();
//        in.close();

        IOUtils.copyBytes(in, out, 16, true);

    }

}
