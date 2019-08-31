package com.bd.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * \* Project: HbaseOperation
 * \* Package: com.bd.wc
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-31 10:58:27
 * \* Description:
 * \
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(" ");
        //new StringTokenizer(value.toString(), " ");
        for (String string : splits){
            context.write(new Text(string), new IntWritable(1));
        }
    }
}