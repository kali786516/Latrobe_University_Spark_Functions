package com.kali.latrobe.uni.spark.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by kalit_000 on 22/12/2015.
 */

object cartesian {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("cartesian").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val x = sc.parallelize(List(1,2,3,4,5))
    val y = sc.parallelize(List(6,7,8,9,10))

    /*
      ANS:-
      (1,7)
      (1,8)
      (1,9)
      (1,10)
      (2,6)
      (2,7)
      (2,8)
      (2,9)
      (2,10)
      (3,6)
      (3,7)
      (3,8)
      (3,9)
      (3,10)
      (4,6)
      (4,7)
      (4,8)
      (4,9)
      (4,10)
      (5,6)
      (5,7)
      (5,8)
      (5,9)
      (5,10)
   */

    x.cartesian(y).collect.foreach(println)



  }

}
