package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object foreachpartition {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)



    val b = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)

    def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }


    /*
    * [partID:0, val: 1]
      [partID:0, val: 2]
      [partID:0, val: 3]
      [partID:1, val: 4]
      [partID:1, val: 5]
      [partID:1, val: 6]
      [partID:2, val: 7]
      [partID:2, val: 8]
      [partID:2, val: 9]
    * */

    b.mapPartitionsWithIndex(myfunc).collect.foreach(println)

    /*6
    15
    24
    * */

   b.foreachPartition(x => println(x.reduce(_ + _)))

  }


}
