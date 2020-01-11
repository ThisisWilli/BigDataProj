package sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2020-01-09 22:08:08
 * \* description:虽然spark.sql.function中已经包含了大多数常用的函数，但是总有很多场景是内置函数无法满足要求的，所以有了自定义函数
 *                 UDF
 * \
 */
public class JavaUDF {

    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("javaCreateSchema2DF")
                .getOrCreate();
        JavaRDD<String> lineRDD = spark.read().textFile("data/person").toJavaRDD();
        // 将数据进行切割，转换成Row类型的RDD
        JavaRDD<Row> rowRDD = lineRDD.map(s -> RowFactory.create(
                String.valueOf(s.split(" ")[0]),
                String.valueOf(s.split(" ")[1]),
                String.valueOf(s.split(" ")[2])
        ));
        // 创建表的元数据
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true))
        );
        // 将数据和元数据合并成为dataframe
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        dataFrame.registerTempTable("ids");
        dataFrame.show();
        spark.udf().register("returnIdLength", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);
        // colName : scala.Predef.String, col : org.apache.spark.sql.Column, metadata : org.apache.spark.sql.types.Metadata
        List<Row> rows = spark.sql("select id, returnIdLength(id) from ids").javaRDD().collect();
        for (Row row : rows){
            System.out.println(row.get(0) + " " + row.get(1));
        }
    }
}