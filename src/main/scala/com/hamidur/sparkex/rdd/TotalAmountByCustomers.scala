package com.hamidur.sparkex.rdd

import org.apache.spark.{SparkConf, SparkContext}

/*
  Sums the total amount spend by the individual customers.

  Data: customer-orders.csv
  Header: customerId, itemId, amount
 */
object TotalAmountByCustomers {

  def main(args: Array[String]) : Unit = {

    val context = new SparkContext(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[3]"))

    context.textFile("data/customer-orders.csv")
      .map(line => {
        val parts = line.split(",")
        (parts(0).toInt, parts(2).toFloat)
      })
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach(println)

    context.stop()
  }

}
