package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object join {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val d = c.keyBy(_.length)

    /* join needs data to have key value pair so use keyBy function to create keys....
    *   (6,(salmon,salmon))
        (6,(salmon,rabbit))
        (6,(salmon,turkey))
        (6,(salmon,salmon))
        (6,(salmon,rabbit))
        (6,(salmon,turkey))
        (3,(dog,dog))
        (3,(dog,cat))
        (3,(dog,gnu))
        (3,(dog,bee))
        (3,(rat,dog))
        (3,(rat,cat))
        (3,(rat,gnu))
        (3,(rat,bee))
    * */

    b.join(d).collect.foreach(println)



  }


}
