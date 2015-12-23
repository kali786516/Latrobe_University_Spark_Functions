package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object Filterwith {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 9, 3)
    val b = a.filterWith(i => i)((x,i) => x % 2 == 0 || i % 2 == 0)
    /*2
      3
      4
      6
      7
      8
9
    * */

    b.collect.foreach(println)


  }


}
