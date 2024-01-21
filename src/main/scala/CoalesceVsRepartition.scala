package com.ahmed

import org.apache.spark.sql.SparkSession

object CoalesceVsRepartition {

  private val spark = SparkSession.builder().master("local[*]").appName("Repartition and Coalesce").getOrCreate()

  private val sc = spark.sparkContext

  private val numbers = sc.parallelize(1 to 10000000)

  /*
      Repartition will always result in an Exchange(Shuffle) of the data.
      Repartition is used if you need to have a more uniform data distribution
   */
  private val repartitionedNumbers = numbers.repartition(2)
  repartitionedNumbers.count()

  /*
    Coalesce on the other hand will only Exchange(Shuffle) data if the number of new partitions is higher than the current.
    Otherwise, it will "Union" together partitions from parents.

    Data distribution is not guaranteed to be uniform. Some partitions will be much bigger than others.
   */
  private val coalescedNumbers = numbers.coalesce(2)
  coalescedNumbers.count()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}