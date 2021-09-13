package com.hamidur.sparkex.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
  Counts the total number of unique words.

  Data: book.txt
 */
object WordCount {

  def main(args: Array[String]) : Unit = {

    val context = new SparkContext(new SparkConf().setAppName(this.getClass.toString).setMaster("local[3]"))

    val contentRDD   = context.textFile("data/book.txt")

//      contentRDD.map(line => line.toLowerCase)
//        .flatMap(line => line.split(" "))
//        .countByValue()
//        .foreach(println)

    countAndSortDifferentApproach(contentRDD)

    context.stop()
  }

  def countAndSortDifferentApproach(contentRDD: RDD[String]) : Unit = {

    contentRDD.flatMap(line => line.toLowerCase.split(" "))
      .map(word => (word, 1))             // make a K/V RDD for each word count (word, count) with initial count as 1
      .reduceByKey((x, y) => x + y)       // sums up the the occurrence by word
      .map(word => (word._2, word._1))    // flip the tuple to (count, word) to use sortByKey func
      .sortByKey(ascending = false)       // sort by most occurred word
      .collect()                          // collect to an array
      .take(5)                            // take top 5 most occurred words
      .foreach(println)                   // print them
  }

}
