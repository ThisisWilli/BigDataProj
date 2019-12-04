package sql;

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-04 11:14:21
 * \* description:
 * \
 */

import bean.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

/**
 * 通过反射的方式将非json格式的数据转化为RDD，其中SparkContext和SparkSession只能出现一个
 */
public class readNonJsonRDDCreateDF {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("readNonJsonRDDCreateDF")
                .master("local")
                .getOrCreate();
        // JavaSparkContext sc = new JavaSparkContext();
        final Dataset<String> stringDataset = spark.read().textFile("data/person");
        //JavaRDD<String> stringRDD = sc.textFile("data/person", 1);
        JavaRDD<Person> mapRDD = stringDataset.toJavaRDD().map(new Function<String, Person>() {
            public Person call(String s) throws Exception {
                Person person = new Person();
                person.setName(s.split(" ")[0]);
                person.setSex(s.split(" ")[1]);
                person.setAge(s.split(" ")[2]);
                return person;
            }
        });
        mapRDD.map(new Function<Person, String>() {
            public String call(Person person) throws Exception {
                return person.toString();
            }
        }).foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        // 利用反射机制生成df，即将RDD转换为Dataset
        Dataset<Row> df = spark.createDataFrame(mapRDD, Person.class);
        df.show();

        // 将DataSet转化为JavaRDD，转化为RDD再转化为Person含有Person类型的RDD
        JavaRDD<Row> transRDD = df.javaRDD();
        transRDD.map(new Function<Row, Person>() {
            public Person call(Row row) throws Exception {
                Person person = new Person();
                person.setName((String)row.getAs("name"));
                person.setAge((String)row.getAs("age"));
                person.setSex((String)row.getAs("sex"));
                return person;
            }
        }).foreach(new VoidFunction<Person>() {
            public void call(Person person) throws Exception {
                System.out.println(person.toString());
            }
        });
    }
}