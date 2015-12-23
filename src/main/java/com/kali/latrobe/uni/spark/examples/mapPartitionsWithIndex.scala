package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object mapPartitionsWithIndex {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val x = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
    def myfunc(index: Int, iter: Iterator[Int]) : Iterator[String] = {
      iter.toList.map(x => index + "," + x).iterator
    }

    /*  partition id with values
    0,1
    0,2
    0,3
    1,4
    1,5
    1,6
    2,7
    2,8
    2,9
    2,10
    * */


    x.mapPartitionsWithIndex(myfunc).collect().foreach(println)



  }


}
