package com.bd.hbase;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * \* Project: HbaseOperation
 * \* Package: com.bd.hbase
 * \* Author: Hoodie_Willi
 * \* Date: 2019-08-30 10:04:32
 * \* Description:
 * \
 */
public class PhoneCase {
    //表管理类
    HBaseAdmin admin = null;
    //数据管理类
    HTable table = null;
    //表名
    String tm = "phone";

    ResultScanner scanner = null;

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
        // 表的描述类
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tm));
        // 创建列族
        HColumnDescriptor family = new HColumnDescriptor("cf".getBytes());
        desc.addFamily(family);
        // 判断表是否存在
        if (admin.tableExists(tm)){
            admin.disableTable(tm);
            admin.deleteTable(tm);
        }
        // 创建表
        admin.createTable(desc);
    }

    /**
     * 10个用户，每个用户每年产生1000条通话记录
     * dnum:对方手机号
     * type:类型：0主叫，1被叫
     * length：长度
     * data：时间
     */
    @Test
    public void insert() throws ParseException, InterruptedIOException, RetriesExhaustedWithDetailsException {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++){
            String phoneNumber = getPhone("158");
            for (int j = 0; j< 1000; j++){
                // 属性
                String dnum = getPhone("177");
                String length = String.valueOf(r.nextInt(99)); // 产生[0, n)的随机数
                String type = String.valueOf(r.nextInt(2)); // [0, 2)
                String date = getDate("2019");
                //rowkey设计
                String rowkey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
                Put put = new Put(rowkey.getBytes());
                put.add("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
                put.add("cf".getBytes(), "length".getBytes(), length.getBytes());
                put.add("cf".getBytes(), "type".getBytes(), type.getBytes());
                put.add("cf".getBytes(), "date".getBytes(), date.getBytes());
                puts.add(put);
            }
        }
        // 全部插入
        table.put(puts);
    }

    /**
     * 10个用户，每个用户1000条，每一条记录当作一个对象进行存储
     */
    @Test
    public void insert2() throws ParseException, InterruptedIOException, RetriesExhaustedWithDetailsException {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++){
            String phoneNumber = getPhone("158");
            for (int j = 0; j < 1000; j++){
                String dnum = getPhone("177");
                String length = String.valueOf(r.nextInt(99)); // 产生[0, n)的随机数
                String type = String.valueOf(r.nextInt(2)); // [0, 2)
                String date = getDate("2019");

                // 保存到对象属性中
                Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
                phoneDetail.setDate(date);
                phoneDetail.setLength(length);
                phoneDetail.setType(type);
                phoneDetail.setDnum(dnum);

                //rowkey
                String rowkey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());
                Put put = new Put(rowkey.getBytes());
                put.add("cf".getBytes(), "phoneDetail".getBytes(), phoneDetail.build().toByteArray());
                puts.add(put);
;            }
        }
        table.put(puts);
    }

    /**
     * 10个用户， 每天产生1000条记录，每天的所有数据放到一个rowkey中
     */
    @Test
    public void insert3() throws ParseException, InterruptedIOException, RetriesExhaustedWithDetailsException {
        List<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 10; i++){
            String phoneNumber = getPhone("158");
            String rowkey = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20190402000000").getTime());
            Phone.DayOfPhone.Builder dayOfPhone = Phone.DayOfPhone.newBuilder();
            for (int j = 0; j < 1000; j++){
                String dnum = getPhone("177");
                String length = String.valueOf(r.nextInt(99)); // 产生[0, n)的随机数
                String type = String.valueOf(r.nextInt(2)); // [0, 2)
                String date = getDate("20190402");

                // 保存到对象属性中
                Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
                phoneDetail.setDate(date);
                phoneDetail.setLength(length);
                phoneDetail.setType(type);
                phoneDetail.setDnum(dnum);
                dayOfPhone.addDayOfPhone(phoneDetail);

                //rowkey
                Put put = new Put(rowkey.getBytes());
                put.add("cf".getBytes(), "day".getBytes(), dayOfPhone.build().toByteArray());
                puts.add(put);
            }
        }
        table.put(puts);
    }

    @Test
    public void get() throws IOException {
        Get get = new Get("15871044379_9223370490668637807".getBytes());
        table.get(get);
        Result result = table.get(get);
        Phone.PhoneDetail phoneDetail = Phone.PhoneDetail.parseFrom(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(),
                "phoneDetail".getBytes())));
        System.out.println(phoneDetail);
    }

    @Test
    public void get2() throws IOException {
        Get get = new Get("15899017841_9223370482720375807".getBytes());
        table.get(get);
        Result result = table.get(get);
        Phone.DayOfPhone parseFrom = Phone.DayOfPhone.parseFrom(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(),
                "day".getBytes())));
        int count = 0;
        for (Phone.PhoneDetail pd : parseFrom.getDayOfPhoneList()) {
            System.out.println(pd);
            count++;
        }
        System.out.println(count);
    }

    /**
     * 查询某一个用户3月份的所有通话记录
     * 条件：
     *  1.某一个用户
     *  2.时间
     */
    @Test
    public void scan1() throws ParseException, IOException {
        String phoneNumber = "15891411775";
        String startRow = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20190401000000").getTime());
        String stopRow = phoneNumber + "_" + (Long.MAX_VALUE - sdf.parse("20190301000000").getTime());
        Scan scan = new Scan();
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());
        table.getScanner(scan);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.print(Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("--" + Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.print("--" + Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
            System.out.println("--" + Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
        }
    }

    /**
     * 查询每一个用户，所有的主叫电话
     * 条件：
     * 1.电话号码
     * 2.type=0
     */
    @Test
    public void scan2() throws IOException {
        // MUST_PASS_ONE只要scan的数据行符合其中一个filter就可以返回结果(但是必须扫描所有的filter)，
        //另外一种MUST_PASS_ALL必须所有的filter匹配通过才能返回数据行(但是只要有一个filter匹配没通过就算失败，后续的filter停止匹配)
        // 因为要查询所有主叫号码，所以选择MUST_PASS_ALL
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(),
                CompareFilter.CompareOp.EQUAL, "0".getBytes());
        PrefixFilter filter2 = new PrefixFilter("15891411775".getBytes());
        filters.addFilter(filter1);
        filters.addFilter(filter2);
        Scan scan = new Scan();
        scan.setFilter(filters);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner){
            System.out.print(Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
            System.out.print("--" + Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
            System.out.print("--" + Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
            System.out.println("--" + Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
        }
        scanner.close();
    }

    /**
     * 产生时间
     * @param string
     * @return
     */
    private String getDate(String string){
        //月-日-小时-分钟-秒
        return string + String.format("%02d%02d%02d%02d%02d", r.nextInt(12) + 1,
                r.nextInt(31), r.nextInt(24), r.nextInt(60), r.nextInt(60));
    }

    private String getDate2(String string){
        return string + String.format("%02d%02d%02d", r.nextInt(24), r.nextInt(60), r.nextInt(60));
    }

    /**
     * 产生手机号后9位
     */
    Random r = new Random();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
    private String getPhone(String phonePrefix){
        return phonePrefix + String.format("%08d", r.nextInt(99999999));
    }




    /**
     *
     * @throws IOException
     */
    @After
    public void destory() throws IOException {
        if (scanner != null){
            scanner.close();
        }
        if (admin != null){
            admin.close();
        }
    }
}