package com.hamidur.sparkex.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object SpotifyMusicChartAnalysis {

  val zeroWhenEmptyElseNum: String => Int = (s: String) => if (s.strip().isBlank || s.strip().isEmpty) 0 else s.strip().toInt

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                    .builder()
                    .config(new SparkConf().setMaster("local[5]").setAppName(this.getClass.getSimpleName))
                    .getOrCreate()

    val df = spark
              .read
              .format("csv")
              .option("header", "true")
              .option("escape", "\"")
              .load("data/spotify_dataset.csv")

    val cleanedRdd = dfToRdd(df)

    // save to memory for faster recomputing from `cleanedRdd`
    cleanedRdd.cache()

    // 1. Count the number of rows in the RDD
    println(cleanedRdd.count())

    // 2. Top 5 songs that had the most streams, print the # of streams, artist, song name
    topFiveSongsByStreams(cleanedRdd)

    // 3. Count the total number of unique artists
    println(cleanedRdd.map(row => row._5).distinct().count())

    // 4. Which artist has the most followers for a single song, print artist name, song name, followers
    cleanedRdd.map(row => (row._3, row._5, row._6))
      .sortBy(_._2, ascending = false)
      .take(1)(0)

    // 5. Find 5 unique artist whose song were ranked 1 in the chart
    cleanedRdd.map(row => (row._1, row._5))
      .groupByKey()
      .filter(row => row._1 == 1)
      .map(row => row._2.toSet.take(5))
      .foreach(println)

    spark.close()
  }

  // converts a dataframe to a tuple of RDD with specific type and only the columns needed
  def dfToRdd(df: DataFrame) : RDD[(Int, Int, String, Int, String, Int, Int, Int, String)] = {
    df.rdd
      .map(row => (zeroWhenEmptyElseNum(row(1).toString),       // Highest Charting Position
        zeroWhenEmptyElseNum(row(2).toString),                  // Number of Times Charted
        row(4).toString,                                        // Song Name
        zeroWhenEmptyElseNum(row(5).toString.strip().replaceAll(",", "")), // Streams
        row(6).toString,                                        // Artist Name
        zeroWhenEmptyElseNum(row(7).toString),                  // Artist Followers
        zeroWhenEmptyElseNum(row(10).toString),                 // Popularity
        zeroWhenEmptyElseNum(row(11).toString),                 // Duration (ms)
        row(12).toString                                        // Chord
      ))
  }

  // Q2: Top 5 songs that had the most streams, print the # of streams, artist, song name
  def topFiveSongsByStreams(rdd: RDD[(Int, Int, String, Int, String, Int, Int, Int, String)]) : Unit = {
    rdd.map(row => (row._3, row._4, row._5))    // take only song name, streams, artist in this order
      .sortBy(_._2, ascending = false)          // sort by stream, descending
      .take(5)
      .foreach(row => println("Streams: " + row._2 + ", Song: " + row._1 + ", Artist: " + row._3))
  }
}
