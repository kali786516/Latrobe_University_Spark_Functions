package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object filter {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Filter").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 10, 3)

    val b = a.filter(_ % 2 == 0)

    /*
    4
    6
    8
    10
    * */

    b.collect.foreach(println)


    val c = sc.parallelize(List("cat", "horse", 4.0, 3.5, 2, "dog"))

    /*
    is string
    is string
    is integer
    is string
    * */

    c.collect({case a: Int    => "is integer"
    case b: String => "is string" }).collect.foreach(println)


  }

}
