package com.hamidur.sparkex.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AirlinesCount {

  def main(args: Array[String]) : Unit = {

    val spark = SparkSession
                    .builder()
                    .config(new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName))
                    .getOrCreate()

    val linesRDD = spark.sparkContext.textFile("data/airlines.csv")

    // save this to memory for frequent access without recomputing everytime on actions
    linesRDD.cache()

    // print lines count
    println(linesRDD.count())

    // ignore the header line
    val linesWOHeader = linesRDD.filter(line => !line.contains("IATA_CODE"))

    /*
        - map the lines to (code, name) pair
        - sort the data by code name ascending
        - collect the data from the driver
        - print the data to console
     */

    linesWOHeader.map(line => {
      val parts = line.split(",")
      (parts(0), parts(1))
    })
      .sortByKey()
      .collect()
      .foreach({case (k, v) => println(k + " -> " + v)})


    spark.stop()
  }

}
