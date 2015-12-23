package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object countapproxdistinctbykey {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("countapproxdistinctbykey").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    /*
    Similar to countApproxDistinct, but computes the approximate number of distinct values for each distinct key. Hence, the RDD must consist of
    two-component tuples. For large RDDs which are spread across many nodes, this function may execute
    faster than other counting methods. The parameter relativeSD controls the accuracy of the computation.
     */

    val a = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog"), 2)
    val b = sc.parallelize(a.takeSample(true, 10000, 0), 20)
    val c = sc.parallelize(1 to b.count().toInt, 20)
    val d = b.zip(c)
    /*
    (Cat,1)
    (Dog,2)
    (Dog,3)
    (Cat,4)
    (Rat,5)
    (Gnu,6)
    (Rat,7)
    (Gnu,8)
     */

    //d.collect().foreach(println)

    /*gets back distinct values out of large data set*/

    d.countApproxDistinctByKey(0.1).collect.foreach(println)



  }

}
