package com.atguigu.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 接收kv，输出到文件
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public MyRecordWriter(TaskAttemptContext job) throws IOException {

        Configuration configuration = job.getConfiguration();

        String parent = configuration.get(FileOutputFormat.OUTDIR);

        FileSystem fs = FileSystem.get(configuration);

        atguigu = fs.create(new Path(parent + "/atguigu.log"));
        other = fs.create(new Path(parent + "/other.log"));

    }

    /**
     * 接收kv输入，输出到文件
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {

        String line = value.toString() + "\n";

        if (line.contains("atguigu")) {
            atguigu.write(line.getBytes());
        } else {
            other.write(line.getBytes());
        }

    }

    /**
     * 关闭资源
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStreams(atguigu, other);
    }
}
