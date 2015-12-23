package com.kali.latrobe.uni.spark.examples

/**
 * Created by kalit_000 on 22/12/2015.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}


object aggregateByKey {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("AggregateByKey").set("spark.hadoop.validateOutputSpecs", "false")
    val sc=new SparkContext(conf)


    val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)

    // lets have a look at what is in the partitions
    def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
      iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }

    /*
    http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-1/
    Avoid reduceByKey When the input and output value types are different. For example, consider writing a transformation that finds all the unique
    strings corresponding to each key. One way would be to use map to transform each element into a Set and then combine the Sets with reduceByKey:

    */

    pairRDD.mapPartitionsWithIndex(myfunc).collect.foreach(println)

    /*
      [partID:0, val: (cat,2)]
      [partID:0, val: (cat,5)]
      [partID:0, val: (mouse,4)]
      [partID:1, val: (cat,12)]
      [partID:1, val: (dog,12)]
      [partID:1, val: (mouse,2)]

      Max value(partition 0) (cat,5) + Max value (parition 1) (cat,12) = (cat,17)
      Ans:-
      (dog,12)
      (cat,17)
      (mouse,6)
     */

    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(println)

    /*
      [partID:0, val: (cat,2)]
      [partID:0, val: (cat,5)]
      [partID:0, val: (mouse,4)]
      [partID:1, val: (cat,12)]
      [partID:1, val: (dog,12)]
      [partID:1, val: (mouse,2)]

      Max value(partition 0) (cat,5) (replace 5 with 100) + Max value (parition 1) (cat,12) (replace 12 with 100) = (cat,200)
      Max value(partition 0) (mouse,4) (replace 4 with 100) + Max value (parition 1) (mouse,2) (replace 2 with 100) = (mouse,200)

      Ans:-
      (dog,100)
      (cat,200)
      (mouse,200)
     */


    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(println)

  }

}
