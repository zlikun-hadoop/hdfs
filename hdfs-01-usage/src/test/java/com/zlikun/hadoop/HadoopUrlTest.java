package com.zlikun.hadoop;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * 通过URL操作HDFS文件系统
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-14 13:26
 */
public class HadoopUrlTest {

    /**
     * 通过SHELL在HDFS中准备一个文本文件
     * $ echo 'java lua go erlang c c++ python' > lang.txt
     * $ hdfs dfs -mkdir /txt
     * $ hdfs dfs -put lang.txt /txt/
     * $ hdfs dfs -cat /txt/lang.txt
     * java lua go erlang c c++ python
     */
    @Test
    public void test() {

        // 为了让Java程序能识别Hadoop的hdfs URL方案，需要做一些额外工作
        // 下面语句是实现该通力的一种方法，该语句只能在JVM中运行一次，所以一般放静态代码块中
        // 该机制的一个弊端是如果有其它第三方类(库)已经声明了一个URLStreamHandlerFactory实例，将无法再使用该方法从Hadoop中读取数据
        // 每个Java虚拟机只能调用一次该方法
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        // 使用URL读取文件
        InputStream in = null ;
        try {
            in = new URL("hdfs://hadoop.zlikun.com:9000/txt/lang.txt").openStream() ;

            // java lua go erlang c c++ python
            IOUtils.copyBytes(in, System.err, 4096, false);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }

    }

}
