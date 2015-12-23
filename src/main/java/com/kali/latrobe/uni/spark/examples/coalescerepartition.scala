package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 22/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object coalescerepartition {

 def main (args: Array[String]) {

   Logger.getLogger("org").setLevel(Level.WARN)
   Logger.getLogger("akka").setLevel(Level.WARN)

   val conf = new SparkConf().setMaster("local[*]").setAppName("collasecereparition").set("spark.hadoop.validateOutputSpecs", "false")
   val sc = new SparkContext(conf)

   /* below y becomes 10 paritions ....*/

   /*certification question:- colleasce helps in repartition without data shuffle  .....*/

   val y = sc.parallelize(1 to 10, 10)

   /* y paritions are 10 */

   println(y.partitions.length)

   /*coaleasce will cut down to 2 paritions , false is used to say not to shuffle data ....*/

   val z = y.coalesce(2, false)

   println(z.partitions.length)

  }
}
