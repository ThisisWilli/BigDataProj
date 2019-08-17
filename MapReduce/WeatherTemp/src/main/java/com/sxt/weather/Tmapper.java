package com.sxt.weather;

import com.google.inject.internal.cglib.core.$ObjectSwitchCallback;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import sun.util.resources.cldr.bas.CalendarData_bas_CM;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 15:14:09
 * \* Description:
 * \
 */
//        a.泛型一:KEYIN：LongWritable，对应的Mapper的输入key。输入key是每行的行首偏移量
//        b.泛型二: VALUEIN：Text，对应的Mapper的输入Value。输入value是每行的内容
//        c.泛型三:KEYOUT：对应的Mapper的输出key，根据业务来定义
//        d.泛型四:VALUEOUT：对应的Mapper的输出value，根据业务来定义
//        初学时，KEYIN和VALUEIN写死(LongWritable,Text)。KEYOUT和VALUEOUT不固定,根据业务来定
// 1949-10-01 14:21:02 34C
public class Tmapper extends Mapper <LongWritable, Text, Weather, IntWritable> {
    Weather tkey = new Weather(); // 处理后的年月日，温度
    IntWritable tval = new IntWritable(); // 封装温度
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //对一行数据进行处理1949-10-01 14:21:02,34C,获得时间温度
        String[] words = StringUtils.split(value.toString(), ','); // 定位制表 符，切割时间和温度
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        try {
            Date date = sdf.parse(words[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            // 处理日期
            tkey.setYear(cal.get(Calendar.YEAR));
            tkey.setMonth(cal.get(Calendar.MONTH) + 1);
            tkey.setDay(cal.get(Calendar.DAY_OF_MONTH));

            // 切割出温度
            int temp = Integer.parseInt(words[1].substring(0, words[1].lastIndexOf("c")));
            tkey.setTemp(temp);
            tval.set(temp);
            context.write(tkey, tval);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}