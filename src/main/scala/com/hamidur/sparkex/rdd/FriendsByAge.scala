package com.hamidur.sparkex.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Counts the average friends by age.

  Data: fakefriends-noheader.csv
  Header Format: id,name,age,friends
 */

object FriendsByAge {

  lazy val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    logger.setLevel(Level.ERROR)

    val sparkContext: SparkContext = new SparkContext(new SparkConf().setMaster("local[5]").setAppName("FriendsByAge"))

    val linesRdd = sparkContext.textFile("data/fakefriends-noheader.csv")

    // process each line to (age, friends) tuple of RDD
    val ageFriendsRdd = linesRdd.map(lineToAgeFriends)

    /*
      process only the value part to (value, 1) tuple while keeping the key as is.
      ex: (30, (270, 1)), (31, (150, 1)), (30, (340, 1))
      where 1 is used for further processing/reducing.
      1 will be used for counting how many times a certain age number has occurred so average can be computed.
     */
    val tempAgeFriends = ageFriendsRdd.mapValues(value => (value, 1))

    /*
      reducing by unique key (age).
      takes 2 rows/tuples (value part only) from the rdd and sum the friends and 1s by unique age.
      ex: (30, (270, 1)), (31, (150, 1)), (30, (130, 1)) will be transformed into
      (30, (400, 2)), (31, (150, 1)) now we can divide by tuple._2 to find the average
     */
    val uniqueAgeFriendsRdd = tempAgeFriends.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // finds the average
    val averageFriends = uniqueAgeFriendsRdd.mapValues(x => x._1 / x._2)

    // collect data from RDD to collections, sort by age ascending and print
    averageFriends.collect().sortBy(_._1).foreach(println)

    sparkContext.stop()
  }

  def lineToAgeFriends(line: String) : (Int, Int) = {
    val splits = line.split(",")
    (splits(2).toInt, splits(3).toInt)
  }

}
