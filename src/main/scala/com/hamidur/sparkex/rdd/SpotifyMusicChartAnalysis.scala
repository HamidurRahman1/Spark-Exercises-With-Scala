package com.hamidur.sparkex.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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

    cleanedRdd.take(10).foreach(println)

    spark.close()
  }

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
}
