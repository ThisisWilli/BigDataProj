package com.sxt.FR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * \* Project: FriendRecommended
 * \* Package: com.sxt.FR
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-17 13:27:40
 * \* Description:
 * \
 */

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Text tkey = new Text();
    IntWritable tval = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // world hadoop hello hive
        // 切割数据
        String[] words = StringUtils.split(value.toString(), ' ');

        for (int i = 1; i < words.length; i++){
            // 把一对封装在tkey中
            tkey.set(getFd(words[0], words[i]));
            // 如果是直接好友，则直接输出0
            tval.set(0);
            // 用数组的第一个元素与后面的所有元素一一匹配，输出他们他们的直接好友关系
            context.write(tkey, tval);

            for (int j = i + 1; j < words.length; j++){
                // 把一对儿封装在tkey中
                tkey.set(getFd(words[i], words[j]));
                // 如果是潜在间接好友， 则直接输出1
                tval.set(1);
                // 用数组的第一个元素与后面的所有元素一一匹配，输出他们他们的直接好友关系
                context.write(tkey, tval);
            }
        }
    }
    private String getFd(String a, String b){
        return a.compareTo(b) > 0 ? b+":"+a : a+":"+b;
    }
}