package com.sxt.FR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * \* Project: FriendRecommended
 * \* Package: com.sxt.FR
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-17 13:15:31
 * \* Description:
 * \
 */
public class MyFD {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyFD.class);
        job.setJobName("friend");

        Path inPath = new Path("/user/fd/input");
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path("/user/fd/output");
        if (outPath.getFileSystem(conf).exists(outPath)){
            outPath.getFileSystem(conf).delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(FReducer.class);
        job.waitForCompletion(true);
    }
}