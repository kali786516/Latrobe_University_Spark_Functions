package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object CollectAsMap  {
def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("collectasmap").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.zip(a)

  /* dont use collectAsMap function this runs collect function first which runs on driver
  *  this function doesnt run in parallel
  * */

  b.collectAsMap.foreach(println)

  /* alternate solution*/

  println("Alternative solution")

  b.reduceByKey((_, v) => v).foreach(println)




  }
}
