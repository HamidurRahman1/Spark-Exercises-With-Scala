package com.hamidur.sparkex.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AirportAnalysis {

  def main(args: Array[String]) : Unit = {

    // create the spark session object with SparkConf object
    val spark = SparkSession
                  .builder()
                  .config(new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[5]"))
                  .getOrCreate()

    // read airports.csv into String RDD with header
    val airportsRDD: RDD[String] = spark.sparkContext.textFile("data/airports.csv")

    /*
      - remove the header line
      - ignore Country, LATITUDE and LONGITUDE (all airports are in the USA)
      - create K/V RDD of (airportKey, (airportName, airportCity, airportState))
     */
    val airports = airportsRDD
                              .filter(line => !line.startsWith("IATA_CODE"))
                              .map(line => {
                                val parts = line.split(",")
                                (parts(0), (parts(1), parts(2), parts(3)))
                              })

    // save this to memory for frequent access without recomputing everytime on actions
    airports.cache()

    // 1. Print Total Airports
    println(airports.count())

    // 2. Print 5 airports info
    airports
      .take(5)
      .foreach({case (k, v) => println(s"Key: $k, Name: ${v._1}, City: ${v._2}, State: ${v._3}")})

    // 3. Print all unique states and how many airports does each one have from most to least
    airports
      .map(tuple => (tuple._2._3, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach(println)


    // 4. Print all airport names and group them by state and see how many airports does each state have
    airports
      .map(tuple => (tuple._2._3, tuple._2._1))
      .groupByKey()
      .sortByKey()
      .collect()
      .foreach({case (state, names) => println(state + " -> " + names.size + " => " + names.toString())})

    // stop the session
    spark.stop()
  }

}
