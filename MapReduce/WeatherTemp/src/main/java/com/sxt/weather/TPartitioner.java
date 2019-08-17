package com.sxt.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 16:05:56
 * \* Description:
 * \
 */
public class TPartitioner extends Partitioner<Weather, IntWritable> {
    @Override
    /**
     * i与reducetask有直接关系
     */
    public int getPartition(Weather weather, IntWritable intWritable, int i) {
        return weather.getYear() % i;
    }
}