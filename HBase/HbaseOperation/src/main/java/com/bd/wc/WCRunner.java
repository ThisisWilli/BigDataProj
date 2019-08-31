package com.bd.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * \* Project: HbaseOperation
 * \* Package: com.bd.wc
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-31 10:58:04
 * \* Description:
 * \
 */
public class WCRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 配置文件
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node02, node03, node04");
        //使用本地模式
        conf.set("fs.defaultFS", "hdfs://node01:8020");

        Job job= Job.getInstance(conf);
        job.setJarByClass(WCRunner.class);

        job.setMapperClass(WCMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        TableMapReduceUtil.initTableReducerJob("wc", WCReducer.class, job, null,
                null, null, null,  false);
        FileInputFormat.addInputPath(job, new Path("/usr/wc"));

        //reduce端输出的key和value的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

//        job.setOutputFormatClass();
//        job.setInputFormatClass();

        job.waitForCompletion(true);
    }
}