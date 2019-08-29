package com.bd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * \* Project: HbaseOperation
 * \* Package: com.bd.hbase
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-29 10:45:30
 * \* Description:
 * \
 */
public class HBaseDemo {

    //表管理类
    HBaseAdmin admin = null;
    //数据管理类
    HTable table = null;
    //表名
    String tm = "phone";

    /**
     * 完成初始化
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node02, node03, node04");
        admin = new HBaseAdmin(conf);
        table = new HTable(conf, tm.getBytes());
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        //表的描述类
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tm));
        //列族
        HColumnDescriptor family = new HColumnDescriptor("cf".getBytes());
        desc.addFamily(family);
        if (admin.tableExists(tm)){
            admin.disableTable(tm);
            admin.deleteTable(tm);
        }
        admin.createTable(desc);
    }

    @Test
    public void insert() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        //put ’<table name>’,’row_key’,’<colfamily:colname>’,’<value>’
        Put put = new Put("1111".getBytes());
        put.add("cf".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        put.add("cf".getBytes(), "age".getBytes(), "zhangsan".getBytes());
        put.add("cf".getBytes(), "sex".getBytes(), "zhangsan".getBytes());
        table.put(put);
    }

    @Test
    public void get() throws IOException {
        //要注意io，避免io过大，增加过滤操作
        Get get = new Get("1111".getBytes()); // row key
        //添加要获取的列和列族，减少网络的io，相当于在服务器端做了过滤
        get.addColumn("cf".getBytes(), "name".getBytes()); //cf:qualifier
        get.addColumn("cf".getBytes(), "age".getBytes());
        get.addColumn("cf".getBytes(), "sex".getBytes());

        Result result = table.get(get);
        Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
        Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
        Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell1)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell2)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell3)));
    }

    @Test
    public void scan() throws IOException {
        Scan scan = new Scan();
        //scan.setStartRow(star);
        //scan.setStopRow();
        ResultScanner rss = table.getScanner(scan);
        for (Result result : rss) {
            Cell cell1 = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
            Cell cell2 = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
            Cell cell3 = result.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell1)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell2)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell3)));
        }
    }

    /**
     *
     * @throws IOException
     */
    @After
    public void destory() throws IOException {
        if (admin != null){
            admin.close();
        }
    }
}