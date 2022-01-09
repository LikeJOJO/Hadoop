package com.atguigu.comparable2;

import com.atguigu.comparable.CompareMapper;
import com.atguigu.comparable.CompareReducer;
import com.atguigu.comparable.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CompareDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(com.atguigu.comparable.CompareDriver.class);

        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(ComparePartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/output"));
        FileOutputFormat.setOutputPath(job, new Path("d:/output2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
