package com.bd.wc;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * \* Project: HbaseOperation
 * \* Package: com.bd.wc
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-31 10:58:42
 * \* Description:
 * \
 */
public class WCReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : iter){
            sum += intWritable.get();
        }
        Put put = new Put(key.getBytes());
        put.add("cf".getBytes(), "cf".getBytes(), String.valueOf(sum).getBytes());
        context.write(null, put);
    }

}