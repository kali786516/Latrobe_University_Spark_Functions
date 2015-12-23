package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object mapvalues {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

   /*
   * so we get only values not the keys
   * */


    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))

    /* length and values
    * (3,xdogx)
      (5,xtigerx)
      (4,xlionx)
      (3,xcatx)
      (7,xpantherx)
      (5,xeaglex)
    * */

    b.mapValues("x" + _ + "x").collect.foreach(println)



  }

}
