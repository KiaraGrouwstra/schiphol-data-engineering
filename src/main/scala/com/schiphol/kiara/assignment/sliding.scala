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

  def main(args: Array[String]): Unit = {
    // delete output directory if exists
    val outPath = "./data/out/window"
    new Directory(new File(outPath)).deleteRecursively()

    val ds = readRoutesStream()
      .transform(cleanRoutes)
    val query = aggregateWindow(ds)
    val fileWriter = writePartitionedStream(outPath)(query)
    val printWriter = printStream(
      query
        .sort(col("count").desc)
    )
    fileWriter.awaitTermination()
    printWriter.awaitTermination()
  }

  // aggregate a stream of flight routes to tally source airports used.
  // the aggregations are done using sliding windows. Pick any window and sliding interval.
  // The end result should be the top 10 airports used as source airport within each window.
  // When choosing the window interval, keep the size of the dataset in mind.
  def aggregateWindow(ds: Dataset[FlightRoute]): Dataset[Row] = {
    val windowString = "5 seconds"
    val slideString = "1 second"
    val timeWindow = window(col("timestamp"), windowString, slideString)
    ds
      .toDF()
      // use randomized timestamps over a range to make distinct values we can slide a window over
      .withColumn("timestamp", (current_timestamp().cast(IntegerType) + round(rand() * 60, 0).cast(IntegerType)).cast(TimestampType))
      .withWatermark("timestamp", "2 seconds")
      // .withColumn("year", year(col("timestamp")))
      // .withColumn("month", format_string("%02d", month(col("timestamp"))))
      // .withColumn("day",   format_string("%02d", dayofmonth(col("timestamp"))))
      // .withColumn("hour",  format_string("%02d", hour(col("timestamp"))))
      // a window of 1 hour would include 3600 entries, i.e. around 5% of the dataset.
      // as a sliding window we will (arbitrarily) pick half of that,
      // such as to ensure we will not get too much data,
      // as we might using a small sliding window.
      // .groupBy(col("timestamp"), col("year"), col("month"), col("day"), col("hour"), col("srcAirport"))
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
      .option("checkpointLocation", "/tmp/checkpoints/")
      // .partitionBy("year", "month", "day", "hour")
      .partitionBy("window")
      .start()
  }

}
