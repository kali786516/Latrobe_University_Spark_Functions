package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object combinebykey {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("combinebykey").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)

    val c = b.zip(a)

    /*
    * (1,dog)
      (1,cat)
      (2,gnu)
      (2,salmon)
      (2,rabbit)
      (1,turkey)
      (2,wolf)
      (2,bear)
      (2,bee)
    * */

    c.collect().foreach(println)


    val d = c.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)

    /* Ans:- (this can be usd to group customer id and their items together ....or group managers with their sub ordinates .....)
    * (1,List(cat, dog, turkey))
      (2,List(gnu, rabbit, salmon, bee, bear, wolf))
    * */

    d.collect.foreach(println)



  }

}
