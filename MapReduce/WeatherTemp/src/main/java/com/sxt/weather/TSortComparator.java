package com.sxt.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 15:37:55
 * \* Description:实现天气年月正序，温度倒序
 * \
 */
public class TSortComparator extends WritableComparator {
    Weather t1 = null;
    Weather t2 = null;

    public TSortComparator() {
        super(Weather.class, true);
    }

    @Override
    /**
     * 保证相同的时间的数据被分到一个reduce
     */
    public int compare(WritableComparable a, WritableComparable b) {
        t1 = (Weather) a;
        t2 = (Weather) b;
        int c1 = Integer.compare(t1.getYear(), t2.getYear());
        if (c1 == 0){
            int c2 = Integer.compare(t1.getMonth(), t2.getMonth());
            if (c2 == 0){
                // 添加-保证高的温度在前，低的温度在后
                return -Integer.compare(t1.getTemp(), t2.getTemp());
            }
            return c2;
        }
        return c1;
    }
}