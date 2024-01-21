package com.ahmed

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.lang

object BroadcastJoins {

  private val spark = SparkSession
    .builder()
    .appName("Broadcast Join")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  private val sc = spark.sparkContext

  private val table: Dataset[lang.Long] = spark.range(1, 100_000_000)
  private val rows: RDD[Row] = sc.parallelize(
    List(
      Row(1, "gold"),
      Row(2, "silver"),
      Row(3, "bronze")
    )
  )

  private val rowSchema = StructType(
    Array(
      StructField("id", IntegerType),
      StructField("medal", StringType)
    )
  )

  private val lookupTable: DataFrame = spark.createDataFrame(rows, rowSchema)

  private val joined = table.join(lookupTable, "id")
  joined.explain()
//  joined.show()

  private val joinedBroadcast = table.join(broadcast(lookupTable), "id")
  joinedBroadcast.explain()
  joinedBroadcast.show()

  def main(args: Array[String]): Unit = {
    Thread.sleep(100000)
  }
}
