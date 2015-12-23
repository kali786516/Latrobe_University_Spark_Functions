package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object countapproxdistinct {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("countapproxdistinct").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    /*For large RDDs which are spread across many nodes, this function may execute faster than other counting methods. The parameter relativeSD controls the accuracy of the computation.*/

    /*fastest method to count values for larger RDD*/

    val a = sc.parallelize(1 to 10000, 20)
    val b = a++a++a++a++a

   println(b.countApproxDistinct(0.1))


    println(b.countApproxDistinct(0.05))


    println(b.countApproxDistinct(0.01))


    println(b.countApproxDistinct(0.001)) // 1000  so SD value 0.001 gives correct answer ...


  }

}
