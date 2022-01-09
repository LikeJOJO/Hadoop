package com.atguigu.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    //声明计数器
    private Counter pass;
    private Counter fail;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //初始化计数器
        pass = context.getCounter(ETLCounter.PASS);
        fail = context.getCounter(ETLCounter.FAIL);


    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 获取一行日志并且切分
        String[] fields = value.toString().split(" ");

        //2. 判断字段个数
        if (fields.length > 11) {
            context.write(value, NullWritable.get());
            pass.increment(1);
        } else
            fail.increment(1);
    }
}
