package com.atguigu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 切开行
        String[] words = value.toString().split(" ");

        //2. 遍历单词输出
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, this.one);
//            context.write(new Text(word), new IntWritable(1));
        }
    }
}
