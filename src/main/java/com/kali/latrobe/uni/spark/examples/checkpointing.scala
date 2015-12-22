package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 22/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object checkpointing {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test_app").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    /*spark streaming usses checkpointing logic ...*/

    sc.setCheckpointDir("C:\\Users\\kalit_000\\Desktop\\typesafe\\latrobe_university_spark")

    val a = sc.parallelize(1 to 4)
    a.checkpoint

    println(a.count)



  }

}
