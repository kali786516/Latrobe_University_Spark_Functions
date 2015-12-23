package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object collect {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("collect").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)

    /*collect function sucks try to avoid as much as you can ...., it needs to collect all the data and get back to driver
    * collect doesnt run in parallel mode, it runs in suck mode (:-) )
    * kali charan once had issue with collect he really nailed it
    * */

    c.collect.foreach(println)






  }

}
