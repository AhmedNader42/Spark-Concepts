package com.ahmed

import org.apache.spark.sql.SparkSession

object Main {

  private val spark = SparkSession.builder().master("local[*]").appName("Repartition and Coalesce").getOrCreate()

  private val sc = spark.sparkContext

  private val numbers = sc.parallelize(1 to 10000000)

  private val repartitionedNumbers = numbers.repartition(2)
  repartitionedNumbers.count()

  private val coalescedNumbers = numbers.coalesce(2)
  coalescedNumbers.count()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}