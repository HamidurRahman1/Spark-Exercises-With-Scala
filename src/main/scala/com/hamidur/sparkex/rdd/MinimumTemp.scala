package com.hamidur.sparkex.rdd

import scala.math.min
import org.apache.spark.{SparkConf, SparkContext}

/*
  Finds the minimum temperature.

  Data: 1800.csv
 */

object MinimumTemp {

  def main(args: Array[String]) : Unit = {

    val context = new SparkContext(new SparkConf().setMaster("local[5]").setAppName(this.getClass.toString))

    context.textFile("data/1800.csv")                           // read data
                      .map(parseLine)                                 // process each line
                      .filter(line => line._2.toUpperCase == "TMIN")  // filter out based on "TMIN"
                      .map(x => (x._1, x._3))                         // process again to make K/V RDD
                      .reduceByKey((x, y) => min(x, y))               // calculate the min temperature
                      .collect()                                      // collect the results from RDD
                      .foreach(println)                               // print the results

    context.stop()
  }

  def parseLine(line: String) : (String, String, Int) = {
    val parts = line.split(",")
    (parts(0), parts(2), parts(3).toInt)
  }

}
