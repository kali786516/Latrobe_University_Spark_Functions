package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object leftouterjoin {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val d = c.keyBy(_.length)

    /*  need data to be in key value pair format .... also the word sum comes in the second rdd
    (6,(salmon,Some(salmon)))
    (6,(salmon,Some(rabbit)))
    (6,(salmon,Some(turkey)))
    (6,(salmon,Some(salmon)))
    (6,(salmon,Some(rabbit)))
    (6,(salmon,Some(turkey)))
    (3,(dog,Some(dog)))
    (3,(dog,Some(cat)))
    (3,(dog,Some(gnu)))
    (3,(dog,Some(bee)))
    (3,(rat,Some(dog)))
    (3,(rat,Some(cat)))
    (3,(rat,Some(gnu)))
    (3,(rat,Some(bee)))
    (8,(elephant,None))
    * */

    b.leftOuterJoin(d).collect.foreach(println)


  }

}
