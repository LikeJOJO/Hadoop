package com.atguigu.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 生成Job对象
        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        configuration.set("yarn.resourcemanager.hostname", "hadoop103");

        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(args[1]), true);
        fileSystem.close();

        Job job = Job.getInstance(configuration);

        //2. 设置Jar包
//        job.setJarByClass(WcDriver.class);

        job.setJar("C:\\Users\\skiin\\IdeaProjects\\mapreduce0921\\target\\mapreduce0921-1.0-SNAPSHOT.jar");
        //3. 要给Job设置自己写的Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcRedcuer.class);

        //4. 设置数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WcRedcuer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //5. 设置输入输出文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long start = System.currentTimeMillis();

        //6. 提交任务
        boolean b = job.waitForCompletion(true);

        long stop = System.currentTimeMillis();

        System.out.println(stop - start);

        System.exit(b ? 0 : 1);


    }
}
