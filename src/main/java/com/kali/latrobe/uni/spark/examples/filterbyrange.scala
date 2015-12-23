package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object filterbyrange {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("filterbyrange").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    /*Returns an RDD containing only the items in the key range specified. From our testing,
    it appears this only works if your data is in key value pairs and it has already been sorted by key.*/

    val randRDD = sc.parallelize(List( (2,"cat"), (6, "mouse"),(7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
    val sortedRDD = randRDD.sortByKey()

    /*looks like filter on key ....*/
    sortedRDD.filterByRange(1, 3).collect.foreach(println)


  }


}
