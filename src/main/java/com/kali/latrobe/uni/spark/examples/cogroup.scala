package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 22/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object cogroup {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("cogroup").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.map((_, "b"))
    val c = a.map((_, "c"))

    /*
    RDD b data:-
    (1,b)
    (2,b)
    (1,b)
    (3,b)
    */

    b.foreach(println)

    /*
    RDD c data:-
    (1,c)
    (2,c)
    (1,c)
    (3,c)
    */

    c.foreach(println)

    /*
    this can be used to find manager and their team members name ......, hierarchy strcuture .....
    Final result:-
    (1,(CompactBuffer(b, b),CompactBuffer(c, c)))  = 2 b and 2 c
    (3,(CompactBuffer(b),CompactBuffer(c)))        = 1 b and 1 c
    (2,(CompactBuffer(b),CompactBuffer(c)))        = 1 b and 1 c
    */

    b.cogroup(c).collect.foreach(println)




  }

}
