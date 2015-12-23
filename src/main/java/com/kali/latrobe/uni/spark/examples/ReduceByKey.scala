package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object ReduceByKey {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("YOUR_APP_NAME_USER").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    val a=sc.parallelize(List("dog","pig","boobs"),2)

    val b=a.map(x => (x.length,x))

    /*
    * (3,dogpig)
     (5,boobs)
    *
    * */


    b.reduceByKey(_+_).collect.foreach(println)


  }


}
