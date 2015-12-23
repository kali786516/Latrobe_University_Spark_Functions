package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object countbykey {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("collect").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val c = sc.parallelize(List((3, "Gnu"), (3, "Yak"), (5, "Mouse"), (3, "Dog")), 2)

    /* countbykey can be used for word count example ....
    * (3,3)
      (5,1)
    * */

    c.countByKey.foreach(println)

  }

}
