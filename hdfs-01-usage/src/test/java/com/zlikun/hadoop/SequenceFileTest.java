package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-14 16:16
 */
public class SequenceFileTest {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    @Test
    public void test() throws IOException {

        // 设置用户名 ( 解决权限问题 )
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // 写文件
        // 可以通过命令：hadoop fs -text /txt/server.txt | head 来读取文件
        write(conf, fs, "/txt/server.txt");

        // 读文件
        read(conf, fs, "/txt/server.txt");

    }

    /**
     * 读SequenceFile文件
     * @param conf
     * @param fs
     * @param path
     */
    private void read(Configuration conf, FileSystem fs, String path) throws IOException {

        SequenceFile.Reader.Option option = SequenceFile.Reader.file(new Path(path));
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, option);

        IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        long position = reader.getPosition();
        while (reader.next(key, value)) {
            String syncSeen = reader.syncSeen() ? "*" : "";
            System.err.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
            position = reader.getPosition(); // beginning of next record
        }
        IOUtils.closeStream(reader);

    }

    /**
     * 写SequenceFile
     * @param conf
     * @param fs
     * @param path
     * @throws IOException
     */
    private void write(Configuration conf, FileSystem fs, String path) throws IOException {
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        SequenceFile.Writer.Option option = SequenceFile.Writer.file(new Path(path));
        SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(value.getClass());
        try {
            writer = SequenceFile.createWriter(conf, option, option2, option3);
            for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.err.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }
    }

}
