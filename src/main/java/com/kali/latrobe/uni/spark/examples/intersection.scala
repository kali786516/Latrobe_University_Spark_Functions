package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object intersection {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val x = sc.parallelize(1 to 20)
    val y = sc.parallelize(10 to 30)
    val z = x.intersection(y)

    /*
    * 19
      14
      13
      17
      18
      16
      10
      15
      20
      11
      12
    * */


    z.foreach(println)


  }


}
