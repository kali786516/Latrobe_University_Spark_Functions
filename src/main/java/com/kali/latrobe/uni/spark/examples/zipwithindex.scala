package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object zipwithindex {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val z = sc.parallelize(100 to 120, 5)
    val r = z.zipWithIndex

    /*
    * (101,1)
      (102,2)
      (103,3)
      (104,4)
      (105,5)
      (106,6)

    *
    * */


    r.collect.foreach(println)


  }

}
