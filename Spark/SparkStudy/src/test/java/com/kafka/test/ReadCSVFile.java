package com.kafka.test;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * \* project: SparkStudy
 * \* package: com.kafka.test
 * \* author: Willi Wei
 * \* date: 2019-12-23 15:29:23
 * \* description:
 * \
 */
public class ReadCSVFile {
    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("data/test2000.csv"));//换成你的文件名
            reader.readLine();//第一行信息，为标题信息，不用，如果需要，注释掉
            String line = null;
            while((line=reader.readLine())!=null){
                String items[] = line.split(",");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
                for (int i = 0; i < items.length;i++){
                    System.out.print(items[i] + " ");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}