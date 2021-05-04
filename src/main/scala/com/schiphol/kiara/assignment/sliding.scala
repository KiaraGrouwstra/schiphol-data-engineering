package com.schiphol.kiara.assignment

import java.sql.Timestamp
import java.io.File
import scala.reflect.io.Directory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.streaming._
import org.apache.spark.streaming._
import org.apache.spark.sql.catalyst.ScalaReflection
import com.schiphol.kiara.assignment.SparkSessionWrapper
import shared._
import batch._
import streaming._

// Next change your streaming job so the aggregations are done using sliding windows. Pick any
// window and sliding interval. The end result should be the top 10 airports used as source airport
// within each window. When choosing the window interval, keep the size of the dataset in mind.
object sliding extends SparkSessionWrapper {
  import spark.implicits._

  val checkPointDirectory = "data/checkpoint"

  def main(args: Array[String]): Unit = {
    // delete output directory if exists
    val outPath = "./data/out/window"
    new Directory(new File(outPath)).deleteRecursively()

    // remove the checkpoint directory or else it will continue where it left off...
    // TODO: remove it for going into production
    new Directory(new File(checkPointDirectory)).deleteRecursively()

    val ds = readRoutesStream()
      .transform(cleanRoutes)
    val query = aggregateWindow(ds)
    val fileWriter = writePartitionedStream(outPath)(query)
    val printWriter = printStream(
      query
        .sort(col("count").desc)
    )
    fileWriter.awaitTermination(120000)
  }

  // aggregate a stream of flight routes to tally source airports used.
  // the aggregations are done using sliding windows. Pick any window and sliding interval.
  // The end result should be the top 10 airports used as source airport within each window.
  // When choosing the window interval, keep the size of the dataset in mind.
  def aggregateWindow(ds: Dataset[FlightRoute]): Dataset[Row] = {
    // limit the amount of steps we're cutting the data into
    val windowString = "5 seconds"
    val slideString = "1 second"
    val timeWindow = window(col("timestamp"), windowString, slideString)
    ds
      .toDF()
      // use randomized timestamps over a range to make distinct values we can slide a window over
      // ensure the data won't go too far into the future
      .withColumn("timestamp", (current_timestamp().cast(IntegerType) + round(rand() * 60, 0).cast(IntegerType)).cast(TimestampType))
      .withWatermark("timestamp", "10 seconds")
      .groupBy(timeWindow, col("srcAirport"))
      .count()
  }

  // write the stream contents to a csv file by yyyy-mm-dd-hh partitioning
  def writePartitionedStream(outPath: String)(ds: Dataset[Row]) = {
    ds
      .writeStream
      .outputMode("append")
      .option("header", true)
      .format("csv")
      .option("path", outPath)
      .option("checkpointLocation", checkPointDirectory)
      .partitionBy("window")
      .start()
  }

}
