package com.atguigu.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        //1. 包装手机号
        phone.set(fields[1]);

        //2. 包装流量
        flow.set(
                Long.parseLong(fields[fields.length-3]),
                Long.parseLong(fields[fields.length-2])
        );

        context.write(phone, flow);

    }
}
