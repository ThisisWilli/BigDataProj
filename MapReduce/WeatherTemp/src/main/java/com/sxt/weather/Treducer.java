package com.sxt.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 16:16:52
 * \* Description:
 * \
 */
//年月相同
//1949-10-01  38
//1949-10-02  37
//1949-10-03  34
//1949-10-01  34
//            1. Reducer组件用于接收Mapper组件的输出
//            2. reduce的输入key,value需要和mapper的输出key,value类型保持一致
//            3. reduce的输出key,value类型，根据具体业务决定
//            4. reduce收到map的输出，会按相同的key做聚合，形成:key Iterable 形式然后通过reduce方法进行传递
//            5. reduce方法中的Iterable是一次性的，即遍历一次之后，再遍历，里面就没有数据了。所以，在某些业务场景，会涉及到多次操作此迭代器，处理的方法是：①先创建一个List  ②把Iterable装到List ③多次去使用List即可

public class Treducer extends Reducer<Weather, IntWritable, Text, IntWritable> {
    Text tkey = new Text();
    IntWritable tval = new IntWritable();
    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 过滤掉同一天的别的温度，因为第一条数据已经是这一天的最高温
        int flag = 0;
        int day = 0;
        for (IntWritable val : values){
            if (flag == 0){
                tkey.set(key.toString());
                tval.set(val.get());
                context.write(tkey, tval);
                flag++;
                day = key.getDay();
            }
            if (flag != 0 && day != key.getDay()){
                tkey.set(key.toString());
                tval.set(val.get());
                context.write(tkey, tval);
                return;
            }
        }
    }
}