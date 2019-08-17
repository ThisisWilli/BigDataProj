package com.sxt.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * \* Project: WeatherTemp
 * \* Package: com.sxt.weather
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-15 14:53:31
 * \* Description:
 * \
 */
public class Weather implements WritableComparable<Weather> {
    private int year;
    private int month;
    private int day;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }


    //重写writable中得方法
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getYear());
        dataOutput.writeInt(this.getMonth());
        dataOutput.writeInt(this.getDay());
        dataOutput.writeInt(this.getTemp());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.setYear(dataInput.readInt());
        this.setMonth(dataInput.readInt());
        this.setDay(dataInput.readInt());
        this.setTemp(dataInput.readInt());
    }

    public int compareTo(Weather o) {
        // 两个天气进行比较，根据数值类型选择比较器，compare返回0,1,-1
        int c1 = Integer.compare(this.getYear(), o.getYear());
        if (c1==0){
            int c2 = Integer.compare(this.getMonth(), o.getMonth());
            if (c2 == 0){
                return Integer.compare(this.getDay(), o.getDay());
            }
            return c2;
        }
        return c1;
    }


    @Override
    public String toString() {
        return year + '-' + month + "-" + day;
    }
}