package com.bbva.qbah.monitoring

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * Created by E032402 on 07/10/2015.
 *
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */
object SparkStreamingFlume {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumePollingEventCount <host> <port>")
      System.exit(1)
    }
    //    Spark Streaming with FlumeNG - FlumePollingEventCount

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val configuration = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming with FlumeNG - FlumePollingEventCount")
    val sparkStreamingContext = new StreamingContext(configuration,Seconds(1))

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = sparkStreamingContext.socketTextStream(args(0),args(1).toInt, MEMORY_AND_DISK_SER)

    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    sparkStreamingContext.start()             // Start the computation
    sparkStreamingContext.awaitTermination()  // Wait for the computation to terminate
  }
}
