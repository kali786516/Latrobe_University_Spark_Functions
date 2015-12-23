

import org.apache.log4j.Logger

/**
 * Created by kalit_000 on 23/12/2015.
 */

import org.apache.log4j.{Level,Logger}
import org.apache.spark.{SparkContext,SparkConf}


object CountByValue {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("CountByValue").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))

    /* ANS
    (5,1)
    (1,6)
    (6,1)
    (2,3)
    (7,1)
    (3,1)
    (8,1)
    (4,2)
    * */

    b.countByValue.foreach(println)





  }


}
