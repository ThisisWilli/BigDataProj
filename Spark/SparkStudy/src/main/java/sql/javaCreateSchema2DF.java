package sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;


/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-04 15:02:11
 * \* description:通过动态创建Schema的方式创建dataframe
 * \
 */

public class javaCreateSchema2DF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("javaCreateSchema2DF")
                .getOrCreate();
        // 读取文件并将原先读取的DataSet类型文件转化为JavaRDD类型
        JavaRDD<String> lineRDD = spark.read().textFile("data/person").toJavaRDD();
        JavaRDD<Row> rowRDD = lineRDD.map(new Function<String, Row>() {
            public Row call(String s) throws Exception {
                return RowFactory.create(
                        String.valueOf(s.split(" ")[0]),
                        String.valueOf(s.split(" ")[1]),
                        String.valueOf(s.split(" ")[2])
                );
            }
        });
        // 动态创建DataFrame中的元数据，元数据可以是自定义的字符串也可以来自外部数据库
        List <StructField> asList = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(asList);
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);
        dataFrame.show();
    }
}