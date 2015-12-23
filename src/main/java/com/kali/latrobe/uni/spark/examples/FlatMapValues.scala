package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object FlatMapValues {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))

    /*
    (3,dog)
    (5,tiger)
    (4,lion)
    (3,cat)
    (7,panther)
    (5,eagle)
    * */

    b.collect.foreach(println)

    /*
    (3,x)
    (3,d)
    (3,o)
    (3,g)
    * */

    b.flatMapValues("x" + _ + "x").collect.foreach(println)
  }


}
