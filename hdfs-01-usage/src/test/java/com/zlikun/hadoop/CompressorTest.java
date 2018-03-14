package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-14 16:00
 */
public class CompressorTest {

    @Test
    public void test() throws ClassNotFoundException, IOException {

        Configuration conf = new Configuration();

        Class<?> clazz = Class.forName("org.apache.hadoop.io.compress.DeflateCodec");
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, conf);
        CompressionOutputStream out = codec.createOutputStream(System.err);

        // org.apache.hadoop.io.compress.DeflateCodec
        // x��H����Q�HL��/PP (��
        // org.apache.hadoop.io.compress.GzipCodec
        //        �H����Q�HL��/PP V���
        // org.apache.hadoop.io.compress.BZip2Codec
        // BZh91AY&SYcC�  ��` @&�   1 0!��0*�\1���H�
        // org.apache.hadoop.io.compress.Lz4Codec
        //       � Hello, Hadoop !
        // org.apache.hadoop.io.compress.SnappyCodec
        // java.lang.RuntimeException: native snappy library not available: this version of libhadoop was built without snappy support.
        // com.hadoop.compression.lzo.LzopCodec
        // java.lang.ClassNotFoundException: com.hadoop.compression.lzo.LzopCodec
        IOUtils.copyBytes(new ByteArrayInputStream("Hello, Hadoop !".getBytes()), out, 4096, false);
        out.finish();

    }

}
