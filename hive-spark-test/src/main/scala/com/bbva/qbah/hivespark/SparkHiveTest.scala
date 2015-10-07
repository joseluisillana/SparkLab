package com.bbva.qbah.hivespark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by E043307 on 06/10/2015.
 */
object SparkHiveTest {

  def main(args: Array[String]) {
    // Spark context initialization
    val conf = new SparkConf().setAppName("Hive - Spark integration").setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val day = args(0)
    val month = args(1)
    val year = args(2)

    val result = sqlContext.sql("SELECT m.duration as duration, m.url from qbah_logs.logs_raw where year=" + year.toString() + " and  month=" + month.toString() + " and day=" +day.toString()+ " order by duration DESC limit 10")

    result.toJSON.saveAsTextFile("hdfs://HDFStest/tmp/CLOUDERA/spark-output/logs-output.json")
  }
}

