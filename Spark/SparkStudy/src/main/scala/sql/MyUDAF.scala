package sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, IntegerType, StructType, StringType}

/**
 * \* project: SparkStudy
 * \* package: sql
 * \* author: Willi Wei
 * \* date: 2019-12-06 10:04:57
 * \* description: 
 * \*/
class MyUDAF extends UserDefinedAggregateFunction{
  // 输入数据的类型
  override def inputSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("uuuu", StringType, true)))
  }

  // 聚合操作，所处理的数据的类型
  override def bufferSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("xxxs", IntegerType, true)))
  }

  // 最终函数返回值的类型
  override def dataType: DataType = {
    DataTypes.IntegerType
  }

  // 多次运行 相同的输入总是相同的输出，确保一致性
  override def deterministic: Boolean = {
    true
  }

  //
  /**
   * 每个分组数据执行的初始化值，重要
   * 两个部分的初始化
   * 1.map端每个RDD分区内，按照groupby的字段，在RDD每个分区内按照groupby的字段分组，内有个初始化的值
   * 2.在reduce端 给每个group by的分组做初始值
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //每个组，有新的值进来的时候，进行分组对应的聚合值的计算，重要，RDD分区内部合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  // 最后merger的时候，在各个节点上的聚合值，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 计算每个分区中的值
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  // 最后返回一个最终的聚合值要和dataType的类型一一对应
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
