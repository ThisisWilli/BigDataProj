package com.sxt.FR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * \* Project: FriendRecommended
 * \* Package: com.sxt.FR
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-17 13:31:17
 * \* Description:
 * \
 */
public class FReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable tval = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 出现tom:hello 0代表两个人直接认识，不需要再去推荐
        // tom:hello 1
        // tom:hell0 1

        // 只考虑以下这种情况，两人间接认识的情况
        // hadoop:hive 1
        // hadoop:hive 1
        // hadoop:hive 1
        // hadoop:hive 1
        int num = 0;
        for (IntWritable val : values){
            if (val.get() == 0){
                return;
            }
            num++;
        }
        // 最终输出两个人之间的共同好友数
        tval.set(num);
        context.write(key, tval);
    }
}