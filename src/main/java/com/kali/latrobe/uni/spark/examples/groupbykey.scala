package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object groupbykey {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length)

    //a.keyBy(x => x).foreach(println)

    //b.foreach(println)

    /* keyBy is used to create a tuple or key value piar which is later used to join two RDD's
       looks like groupby key function needs a dataset with key value pari data
    * (4,CompactBuffer(lion))
      (6,CompactBuffer(spider))
      (3,CompactBuffer(dog, cat))
      (5,CompactBuffer(tiger, eagle))
    *
    * */

    b.groupByKey.collect.foreach(println)



  }


}
