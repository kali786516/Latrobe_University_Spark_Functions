package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object groupby {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a = sc.parallelize(1 to 9, 3)

    /*
    * (even,CompactBuffer(2, 4, 6, 8))
      (odd,CompactBuffer(1, 3, 5, 7, 9))
    * */

    a.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect.foreach(println)



  }


}
