package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-06 11:20:29
 * \* description:用javaAPI中用UDAF读取数据
 * \
 */

/**
 * 1.首先通过sc.parallelize读取list数组，或通过textfile读取其他数据，获取rdd
 * 2.将rdd通过map算子转化成row类型的javaRDD
 * 3.通过DataTypes.createStructField("name", DataTypes.StringType, true)设置dataframe中的元数据类型，并创建DataTypes.createStructType(asList)
 * 4.最后通过spark.createDataFrame(rowRDD, schema);生成Dataset<Row> dataFrame
 */
public class javaUDAF {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("javaUDAF")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> parallelize = sc.parallelize(
                Arrays.asList("zhangsan","lisi","wangwu","zhangsan","zhangsan","lisi"));
        JavaRDD<Row> rowRDD = parallelize.map(new Function<String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });

        List<StructField> fields = Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        dataFrame.registerTempTable("user");
        /**
         * 注册一个UDAF函数,实现统计相同值得个数
         * 注意：这里可以自定义一个类继承UserDefinedAggregateFunction类也是可以的
         */
        spark.udf().register("StringCount",new UserDefinedAggregateFunction() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            /**
             * 初始化一个内部的自己定义的值,在Aggregate之前每组数据的初始化结果
             */
            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0, 0);
            }

            /**
             * 更新 可以认为一个一个地将组内的字段值传递进来 实现拼接的逻辑
             * buffer.getInt(0)获取的是上一次聚合后的值
             * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合
             * 大聚和发生在reduce端.
             * 这里即是:在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
             */
            @Override
            public void update(MutableAggregationBuffer buffer, Row arg1) {
                buffer.update(0, buffer.getInt(0)+1);

            }
            /**
             * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
             * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
             * buffer1.getInt(0) : 大聚合的时候 上一次聚合后的值
             * buffer2.getInt(0) : 这次计算传入进来的update的结果
             * 这里即是：最后在分布式节点完成后需要进行全局级别的Merge操作
             * 也可以是一个节点里面的多个executor合并
             */
            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
            }
            /**
             * 在进行聚合操作的时候所要处理的数据的结果的类型
             */
            @Override
            public StructType bufferSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bffer111", DataTypes.IntegerType, true)));
            }
            /**
             * 最后返回一个和DataType的类型要一致的类型，返回UDAF最后的计算结果
             */
            @Override
            public Object evaluate(Row row) {
                return row.getInt(0);
            }
            /**
             * 指定UDAF函数计算后返回的结果类型
             */
            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }
            /**
             * 指定输入字段的字段及类型
             */
            @Override
            public StructType inputSchema() {
                return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("nameeee", DataTypes.StringType, true)));
            }
            /**
             * 确保一致性 一般用true,用以标记针对给定的一组输入，UDAF是否总是生成相同的结果。
             */
            @Override
            public boolean deterministic() {
                return true;
            }

        });

        spark.sql("select name ,StringCount(name) as strCount from user group by name").show();


        sc.stop();
    }
}