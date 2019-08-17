package com.sxt.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 16:13:31
 * \* Description:
 * \
 */
public class TGroupComparator extends WritableComparator {
    Weather t1 = null;
    Weather t2 = null;

    public TGroupComparator() {
        super(Weather.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        t1 = (Weather) a;
        t2 = (Weather) b;
        int c1 = Integer.compare(t1.getYear(), t2.getYear());
        if (c1 == 0){
            return Integer.compare(t1.getMonth(), t2.getMonth());
        }
        return c1;
    }
}