package com.hamidur.sparkex

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
  Counts the number of ratings for all movies.
  Data is tab delimited.
  Header: UserId, MovieId, Rating, Timestamp

  Ex. 1 -> X, 2 -> Y, 3 -> Z, 4 -> A, 5 -> B
 */

object RatingsCounter {

  def main(args: Array[String]) : Unit = {

    // setting log to only print the errors, skip others
    Logger.getLogger("movieRatingCounter").setLevel(Level.ERROR)

    // creating a SparkConf obj for context with 5 process for parallel processing
    val conf = new SparkConf().setMaster("local[5]").setAppName("MovieRatingsCounter")

    // creating spark context object from the spark config
    val context = new SparkContext(conf)

    // read the data file as a RDD of text file
    val linesRdd = context.textFile("data/u.data")

    // split each line and take the rating part
    val ratings = linesRdd.map(line => line.split("\t")(2))

    // count all ratings by rating type, returns a Map
    val countedRatings = ratings.countByValue()

    // sort them by rating type and print the results
    countedRatings.toSeq.sortBy(_._1).foreach(println)

    // stop the spark app
    context.stop()
  }

}