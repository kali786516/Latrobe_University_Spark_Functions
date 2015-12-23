package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object mappartition {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(1 to 9, 3)
    def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
      var res = List[(T, T)]()
      var pre = iter.next
      while (iter.hasNext)
      {
        val cur = iter.next;
        res .::= (pre, cur)
        pre = cur;
      }
      res.iterator
    }

    /*
        * (2,3)
          (1,2)
          (5,6)
          (4,5)
          (8,9)
          (7,8)
    *
    * */

    a.mapPartitions(myfunc).collect.foreach(println)


  }


}
