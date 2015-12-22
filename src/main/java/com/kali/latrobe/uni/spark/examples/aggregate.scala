package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 22/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

object aggregate {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test_app").set("spark.hadoop.validateOutputSpecs", "false")
    val sc=new SparkContext(conf)

    val z = sc.parallelize(List(1,2,3,4,5,6), 2)

    // lets first print out the contents of the RDD with partition labels
    def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }

    z.mapPartitionsWithIndex(myfunc).collect.foreach(println)

   // z.zipWithIndex.collect().foreach(println)

    //z.zipWithIndex.map(x => x.swap).foreach(println)

    /*
      [partID:0, val: 1]
      [partID:0, val: 2]
      [partID:0, val: 3]
      [partID:1, val: 4]
      [partID:1, val: 5]
      [partID:1, val: 6]

      max value from parition 0 (3) + max value of parition 1 (6) = 9
    */

    println(z.aggregate(0)(math.max(_, _), _ + _))

    // This example returns 16 since the initial value is 5
    // reduce of partition 0 will be max(5, 1, 2, 3) = 5
    // reduce of partition 1 will be max(5, 4, 5, 6) = 6
    // final reduce across partitions will be 5 + 5 + 6 = 16
    // note the final reduce include the initial value

    /*
      [partID:0, val: 1]
      [partID:0, val: 2]
      [partID:0, val: 3]
      [partID:1, val: 4]
      [partID:1, val: 5]
      [partID:1, val: 6]

      max value from parition 0 (5,1,2,3) (5) + max value of parition 1 (6) + 5 =16
    */
    println(z.aggregate(5)(math.max(_, _), _ + _))

    val j = sc.parallelize(List("a","b","c","d","e","f"),2)

    //lets first print out the contents of the RDD with partition labels
    def myfunc2(index: Int, iter: Iterator[(String)]) : Iterator[String] = {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }

    j.mapPartitionsWithIndex(myfunc2).collect.foreach(println)

    println(j.aggregate("")(_ + _, _+_))//defabc

    // See here how the initial value "x" is applied three times.
    //  - once for each partition
    //  - once when combining all the partitions in the second reduce function.
    /*
      [partID:0, val: a]
      [partID:0, val: b]
      [partID:0, val: c]
      [partID:1, val: d]
      [partID:1, val: e]
      [partID:1, val: f]
      x + (x (values from parition 0)) + (x + (values from partition 1))
     */
   println(j.aggregate("x")(_ + _, _+_))


    val a = sc.parallelize(List("12","23","345","4567"),2)

    a.mapPartitionsWithIndex(myfunc2).collect.foreach(println)


    /*
      [partID:0, val: 12]
      [partID:0, val: 23]
      [partID:1, val: 345]
      [partID:1, val: 4567]

      (max length of 23=2 + max length (4567) = 4 )=24 since its a string ......

    */
    println(a.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)) // ans:- 24

    /*
      [partID:0, val: 12]
      [partID:0, val: 23]
      [partID:1, val: 345]
      [partID:1, val: 4567]

      (min length of 23=01 + max length (4567) = 01 )=11 since its a string ......

    */

    println(a.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y))// ans:- 11


    val b = sc.parallelize(List("12","23","345",""),2)

    b.mapPartitionsWithIndex(myfunc2).collect.foreach(println)

    /*
      [partID:0, val: 12]
      [partID:0, val: 23]
      [partID:1, val: 345]
      [partID:1, val: ]

      (min length of 23=01 + max length (0) = 00 )=01 since its a string ......
   */

    println(b.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y))// ans:- 01




  }

}
