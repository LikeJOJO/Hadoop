package com.atguigu.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<>();

    private Text result = new Text();

    /**
     * 处理pd.txt, 将pd.txt的数据全部缓存到pMap中
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1. 开流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        //获取分布式缓存
        URI[] cacheFiles = context.getCacheFiles();
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFiles[0]));

        //2. 按行处理，缓存进pMap
        //将字节流转换成字符流
        BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream, StandardCharsets.UTF_8));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] fields = line.split("\t");
            pMap.put(fields[0], fields[1]);
        }
        IOUtils.closeStreams(reader);
    }

    /**
     * 处理order.txt的数据
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        result.set(
                fields[0] + "\t"
                + pMap.getOrDefault(fields[1], "NULL") + "\t"
                + fields[2]
        );

        context.write(result, NullWritable.get());
    }
}
