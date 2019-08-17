package com.sxt.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 14:36:59
 * \* Description:
 * \
 */
public class MyTQ {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyTQ.class);
        job.setJobName("tq");

        // 2.设置输入输出路径
        Path inPath = new Path("/user/weather/input");
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path("/user/weather/output");
        if (outPath.getFileSystem(conf).exists(outPath)){
            outPath.getFileSystem(conf).delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 3.设置Mapper
        job.setMapperClass(Tmapper.class);
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class); // 设置输出类型

        // 4.自定义排序比较器
        job.setSortComparatorClass(TSortComparator.class);

        // 5.自定义分区器
        job.setPartitionerClass(TPartitioner.class);

        // 6.自定义组排序
        job.setGroupingComparatorClass(TGroupComparator.class);

        // 7.设置reduceTask数量
        job.setNumReduceTasks(1);

        // 8.设置reduce
        job.setReducerClass(Treducer.class);

        //9/
        job.waitForCompletion(true);
    }
}