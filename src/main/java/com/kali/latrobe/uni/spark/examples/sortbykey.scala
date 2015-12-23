package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object sortbykey {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b = sc.parallelize(1 to a.count.toInt, 2)
    val c = a.zip(b)
    /*
    (ant,5)
    (cat,2)
    (dog,1)
    (gnu,4)
    (owl,3)
    */
    c.sortByKey(true).collect.foreach(println)



  }

}
